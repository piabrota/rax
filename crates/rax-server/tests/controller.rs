//! The task reconciler against the in-memory Substrate server.

use std::time::Duration;

use rax_api::v1alpha1::{
    Condition, ObjectMeta, Profile, ResourceList, ResourcesSpec, Task, TaskSpec, TaskState,
    TaskStatus,
};
use rax_api::{API_VERSION, DEFAULT_TASK_IMAGE, KIND_TASK, reason};
use rax_server::controller::{ControllerOptions, SubstrateActors, TaskReconciler};
use rax_server::{ActorState, Actors, Reconciler};
use rax_substrate::ateapi::{
    self, ActorTemplate, ResourceMetadata, ResourceRequirements, Resources,
};
use rax_substrate::testing::FakeSubstrate;
use rax_substrate::{DEFAULT_CPU_MILLICORES, DEFAULT_MEMORY_BYTES, RetryPolicy};
use tonic::{Code, Status};

async fn setup(default_task_image: &str) -> (FakeSubstrate, TaskReconciler) {
    let fake = FakeSubstrate::new();
    let client = fake.start().await;
    let r = TaskReconciler::new(
        client,
        ControllerOptions {
            default_template: "test-template".into(),
            default_task_image: default_task_image.into(),
            workspace_ready_timeout: Duration::from_millis(200),
            workspace_ready_poll_interval: Duration::from_millis(20),
            template_delete_interval: Duration::from_millis(5),
            ..Default::default()
        },
    );
    (fake, r)
}

fn task(name: &str, image: &str) -> Task {
    Task {
        api_version: API_VERSION.into(),
        kind: KIND_TASK.into(),
        metadata: Some(ObjectMeta {
            name: name.into(),
            atespace: "default".into(),
            ..Default::default()
        }),
        spec: Some(TaskSpec {
            profile: Some(Profile {
                image: image.into(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        status: None,
    }
}

/// A task named `atespace/name`, as delete takes it.
fn named(atespace: &str, name: &str) -> Task {
    let mut t = task(name, "img");
    t.metadata.as_mut().unwrap().atespace = atespace.into();
    t
}

fn status(t: &Task) -> &TaskStatus {
    t.status.as_ref().expect("status")
}

fn condition<'a>(t: &'a Task, kind: &str) -> &'a Condition {
    status(t)
        .conditions
        .iter()
        .find(|c| c.r#type == kind)
        .unwrap_or_else(|| panic!("no {kind} condition in {:?}", status(t).conditions))
}

#[track_caller]
fn assert_condition(t: &Task, kind: &str, want_status: &str, want_reason: &str) {
    let c = condition(t, kind);
    assert_eq!(
        (c.status.as_str(), c.reason.as_str()),
        (want_status, want_reason),
        "{kind}: {c:?}"
    );
}

#[tokio::test]
async fn provisions_and_resumes_a_running_task() {
    let (fake, r) = setup("").await;
    let mut t = task("test-task", "ghrc.io/my-org/my-image");
    t.spec.as_mut().unwrap().commands = vec!["/bin/task-runner".into()];
    // A client-supplied actor name is ignored: the actor is always named
    // after the task.
    t.status = Some(TaskStatus {
        actor: "not-the-task".into(),
        ..Default::default()
    });

    r.resume(&mut t, None, None).await.unwrap();
    let got = t;
    let st = status(&got);
    // The state is the server's to read from the actor.
    assert_eq!(st.state(), TaskState::Unspecified);
    assert_eq!(st.resolved_argv, ["/bin/task-runner"]);
    assert!(
        st.ate_actor_template.starts_with("test-task-tmpl-"),
        "{}",
        st.ate_actor_template
    );
    assert_condition(&got, "WorkspaceReady", "False", reason::INITIALIZING);
    assert!(
        condition(&got, "WorkspaceReady")
            .message
            .ends_with("10.244.1.42")
    );

    let s = fake.state();
    assert_eq!(s.calls_of("CreateAtespace"), ["default"]);
    assert_eq!(s.calls_of("CreateActor"), ["default/test-task"]);
    assert_eq!(s.calls_of("ResumeActor"), ["default/test-task"]);
    let tmpl = &s.created_templates[0];
    let name = &tmpl.metadata.as_ref().unwrap().name;
    assert!(
        rax_server::controller::is_task_template("test-task", name),
        "{name}"
    );
    let env: Vec<_> = tmpl.containers[0]
        .env
        .iter()
        .map(|e| e.name.as_str())
        .collect();
    assert_eq!(env, ["AX_TASK_YAML"]);
    let actor = s.actors.get("default/test-task").unwrap();
    assert_eq!(actor.actor_template.as_ref().unwrap().name, *name);
}

#[tokio::test]
async fn defaults_the_task_image() {
    for (over, want) in [
        ("", DEFAULT_TASK_IMAGE),
        (
            "localhost:5001/ax-task-runner:e2e",
            "localhost:5001/ax-task-runner:e2e",
        ),
    ] {
        let (fake, r) = setup(over).await;
        let mut got = task("no-image", "");
        r.resume(&mut got, None, None).await.unwrap();
        let images: Vec<_> = fake
            .state()
            .created_templates
            .iter()
            .map(|t| t.containers[0].image.clone())
            .collect();
        assert_eq!(images, [want]);
    }
}

#[tokio::test]
async fn new_tasks_are_suspended() {
    let (fake, r) = setup("").await;
    let mut got = task("suspend-task", "img");
    r.suspend(&mut got, None, None).await.unwrap();
    assert_eq!(status(&got).state(), TaskState::Unspecified);
    assert_eq!(
        fake.state().calls_of("SuspendActor"),
        ["default/suspend-task"]
    );
}

#[tokio::test]
async fn a_failed_suspend_keeps_the_status() {
    let (fake, r) = setup("").await;
    fake.state().fail(
        "SuspendActor",
        (0..10).map(|_| Status::unavailable("substrate unavailable")),
    );
    let mut t = task("suspend-fail-task", "img");
    let st = t.status.get_or_insert_default();
    st.conditions.push(Condition {
        r#type: "Ready".into(),
        status: "True".into(),
        reason: reason::TASK_RUNNING.into(),
        ..Default::default()
    });
    let before = t.status.clone();
    let err = r.suspend(&mut t, None, None).await.unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
    assert_eq!(t.status, before, "a failed suspend restores the status");
}

#[tokio::test]
async fn a_rejected_template_fails_the_task_with_substrates_message() {
    const MSG: &str = r#"actor_template.containers[0].image: Invalid value: "localhost:5001/my-agent:v1": must be pinned by digest (changing the image invalidates snapshots)"#;
    let (fake, r) = setup("").await;
    fake.state()
        .fail("CreateActorTemplate", [Status::invalid_argument(MSG)]);
    let mut t = task("tagged-image", "localhost:5001/my-agent:v1");
    t.spec.as_mut().unwrap().commands = vec!["python3".into(), "/app/agent.py".into()];
    let err = r.resume(&mut t, None, None).await.unwrap_err();
    assert!(
        err.message().contains("must be pinned by digest"),
        "{}",
        err.message()
    );
    let got = t;
    assert_condition(&got, "Ready", "False", reason::TEMPLATE_CREATION_FAILED);
    assert_eq!(
        condition(&got, "Ready").message,
        MSG,
        "the bare Substrate message"
    );
    // No fallback to the default template.
    assert!(fake.state().calls_of("CreateActor").is_empty());
    assert!(fake.state().calls_of("ResumeActor").is_empty());
}

#[tokio::test]
async fn a_transient_template_failure_leaves_the_task_pending() {
    let (fake, r) = setup("").await;
    fake.state().fail(
        "CreateActorTemplate",
        [Status::deadline_exceeded(
            "substrate control plane timed out",
        )],
    );
    let mut got = task("template-transient", "img@sha256:00");
    let err = r.resume(&mut got, None, None).await.unwrap_err();
    assert_eq!(err.code(), Code::DeadlineExceeded);
    assert_condition(&got, "Ready", "False", reason::SUBSTRATE_UNAVAILABLE);
    assert!(fake.state().calls_of("CreateActor").is_empty());
}

/// Serves `/readyz` with 200 OK.
async fn ready_server() -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = listener.accept().await {
            let mut buf = [0; 2048];
            let _ = s.read(&mut buf).await;
            let _ = s
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 3\r\nconnection: close\r\n\r\nok\n")
                .await;
        }
    });
    addr
}

#[tokio::test]
async fn workspace_readiness_is_polled_until_it_is_set_up() {
    let (fake, r) = setup("").await;
    let ready = ready_server().await;
    let t = task("ready-task", "");
    fake.state().worker_ip = "127.0.0.1:1".into();

    // The worker does not answer: still initializing.
    let mut got = t.clone();
    r.resume(&mut got, None, None).await.unwrap();
    assert_condition(&got, "WorkspaceReady", "False", reason::INITIALIZING);

    // It answers: set up.
    fake.state().worker_ip = ready.clone();
    let mut got = t;
    r.resume(&mut got, None, None).await.unwrap();
    assert_condition(&got, "WorkspaceReady", "True", reason::SETUP_COMPLETE);
    assert_eq!(
        condition(&got, "WorkspaceReady").message,
        format!("Workspace setup completed at {ready}")
    );

    // Suspending keeps WorkspaceReady.
    r.suspend(&mut got, None, None).await.unwrap();
    assert_condition(&got, "WorkspaceReady", "True", reason::SETUP_COMPLETE);

    // Resuming onto an unreachable worker trusts the recorded readiness.
    fake.state().worker_ip = "127.0.0.1:1".into();
    r.resume(&mut got, None, None).await.unwrap();
    assert_condition(&got, "WorkspaceReady", "True", reason::SETUP_COMPLETE);
    assert_eq!(
        fake.state().templates.len(),
        1,
        "status changes must not make new templates"
    );

    // A launch configuration change gets its own template.
    got.spec.as_mut().unwrap().commands = vec!["python3".into(), "agent.py".into()];
    r.resume(&mut got, None, None).await.unwrap();
    assert_eq!(fake.state().templates.len(), 2);
}

#[tokio::test]
async fn readiness_is_also_probed_through_the_router() {
    let fake = FakeSubstrate::new();
    let client = fake.start().await;
    let router = ready_server().await;
    let r = TaskReconciler::new(
        client,
        ControllerOptions {
            router_addr: router,
            workspace_ready_timeout: Duration::from_secs(5),
            ..Default::default()
        },
    );
    fake.state().worker_ip = "127.0.0.1:1".into();
    let mut got = task("routed", "img");
    r.resume(&mut got, None, None).await.unwrap();
    assert_condition(&got, "WorkspaceReady", "True", reason::SETUP_COMPLETE);
}

fn limits(cpu: i64, memory: i64) -> Option<ResourceRequirements> {
    Some(ResourceRequirements {
        limits: Some(Resources {
            cpu_millicores: cpu,
            memory_bytes: memory,
        }),
    })
}

fn with_limits(mut t: Task, cpu: &str, memory: &str) -> Task {
    t.spec.as_mut().unwrap().profile.as_mut().unwrap().resources = Some(ResourcesSpec {
        limits: Some(ResourceList {
            cpu: cpu.into(),
            memory: memory.into(),
        }),
    });
    t
}

#[tokio::test]
async fn resource_limits_go_on_the_template() {
    let (fake, r) = setup("").await;
    let t = with_limits(task("sized-task", "img"), "2", "4Gi");
    r.suspend(&mut t.clone(), None, None).await.unwrap();
    assert_eq!(
        fake.state().created_templates[0].resources,
        limits(2000, 4 << 30)
    );

    // A new limit is a new launch configuration, so a new template.
    let mut t = with_limits(t, "2", "8Gi");
    r.suspend(&mut t, None, None).await.unwrap();
    {
        let s = fake.state();
        assert_eq!(s.created_templates.len(), 2);
        let name = |t: &ActorTemplate| t.metadata.as_ref().unwrap().name.clone();
        assert_ne!(name(&s.created_templates[0]), name(&s.created_templates[1]));
        assert_eq!(s.created_templates[1].resources, limits(2000, 8 << 30));
    }

    r.suspend(&mut task("plain-task", "img"), None, None)
        .await
        .unwrap();
    assert_eq!(
        fake.state().created_templates.last().unwrap().resources,
        limits(DEFAULT_CPU_MILLICORES, DEFAULT_MEMORY_BYTES)
    );
}

#[tokio::test]
async fn invalid_limits_fail_before_provisioning() {
    let (fake, r) = setup("").await;
    let bad = [
        ResourcesSpec {
            limits: Some(ResourceList {
                cpu: "two".into(),
                ..Default::default()
            }),
        },
        ResourcesSpec {
            limits: Some(ResourceList {
                cpu: "0".into(),
                ..Default::default()
            }),
        },
        ResourcesSpec {
            limits: Some(ResourceList {
                cpu: "1000".into(),
                ..Default::default()
            }),
        },
        ResourcesSpec {
            limits: Some(ResourceList {
                memory: "-4Gi".into(),
                ..Default::default()
            }),
        },
    ];
    for reqs in bad {
        let mut t = task("bad-limits", "img");
        t.spec.as_mut().unwrap().profile.as_mut().unwrap().resources = Some(reqs.clone());
        let err = r.resume(&mut t, None, None).await.unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument, "{reqs:?}");
        assert_condition(&t, "Ready", "False", reason::INVALID_RESOURCES);
    }
    let s = fake.state();
    assert!(s.created_templates.is_empty() && s.calls_of("CreateActor").is_empty());
}

#[tokio::test]
async fn rejected_templates_never_fall_back() {
    for with in [true, false] {
        let (fake, r) = setup("").await;
        fake.state().fail(
            "CreateActorTemplate",
            [Status::invalid_argument("rejected")],
        );
        let mut t = task("rejected", "img");
        if with {
            t = with_limits(t, "2", "4Gi");
        }
        r.suspend(&mut t, None, None).await.unwrap_err();
        assert_condition(&t, "Ready", "False", reason::TEMPLATE_CREATION_FAILED);
        assert!(fake.state().calls_of("CreateActor").is_empty());
    }
}

fn template(name: &str) -> ActorTemplate {
    ActorTemplate {
        metadata: Some(ResourceMetadata {
            atespace: "default".into(),
            name: name.into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn delete_removes_the_actor_and_its_templates_only() {
    let (fake, r) = setup("").await;
    {
        let mut s = fake.state();
        s.put_actor("default", "job", ateapi::ActorState::Running);
        for name in [
            "job-tmpl-0a1b2c3d",               // the current revision of "job"
            "job-tmpl-deadbeef",               // an old revision of "job"
            "job-tmpl-deadbeef-tmpl-01234567", // a task named "job-tmpl-deadbeef"
            "jobs-tmpl-0a1b2c3d",              // the task "jobs"
            "default-template",
        ] {
            s.templates
                .insert(format!("default/{name}"), template(name));
        }
    }
    r.delete(&named("default", "job")).await.unwrap();
    let s = fake.state();
    assert_eq!(s.calls_of("DeleteActor"), ["default/job"]);
    assert_eq!(
        s.calls_of("DeleteActorTemplate"),
        ["default/job-tmpl-0a1b2c3d", "default/job-tmpl-deadbeef"]
    );
    let left: Vec<_> = s.templates.keys().cloned().collect();
    assert_eq!(
        left,
        [
            "default/default-template",
            "default/job-tmpl-deadbeef-tmpl-01234567",
            "default/jobs-tmpl-0a1b2c3d"
        ]
    );
}

#[tokio::test]
async fn delete_waits_for_the_actor_to_go() {
    let (fake, r) = setup("").await;
    fake.state()
        .put_actor("default", "slow", ateapi::ActorState::Running);
    fake.state().deleting_polls = 2;
    r.delete(&named("", "slow")).await.unwrap();
    assert!(fake.state().calls_of("GetActor").len() >= 3);
    assert!(fake.state().actors.is_empty());
}

#[tokio::test]
async fn delete_gives_up_on_an_actor_that_stays() {
    let fake = FakeSubstrate::new();
    let client = fake.start().await.with_retry_policy(RetryPolicy {
        delete_timeout: Duration::from_millis(100),
        ..RetryPolicy::fast()
    });
    let r = TaskReconciler::new(client, ControllerOptions::default());
    fake.state()
        .put_actor("default", "stuck", ateapi::ActorState::Running);
    fake.state().deleting_polls = usize::MAX;
    let err = r.delete(&named("default", "stuck")).await.unwrap_err();
    assert_eq!(err.code(), Code::DeadlineExceeded);
    assert!(
        err.message().contains("context deadline exceeded"),
        "{}",
        err.message()
    );
}

#[tokio::test]
async fn transient_failures_leave_tasks_pending_and_recover() {
    let (fake, r) = setup("").await;

    #[track_caller]
    fn pending(t: &Task, e: Status) {
        assert!(rax_server::is_transient(&e), "{e:?}");
        assert_condition(t, "Ready", "False", reason::SUBSTRATE_UNAVAILABLE);
    }

    fake.state()
        .fail("CreateAtespace", [Status::deadline_exceeded("timed out")]);
    let mut t = task("transient-atespace", "img");
    let e = r.suspend(&mut t, None, None).await.unwrap_err();
    pending(&t, e);

    fake.state()
        .fail("CreateActor", [Status::deadline_exceeded("timed out")]);
    let mut t = task("transient-actor", "img");
    let e = r.suspend(&mut t, None, None).await.unwrap_err();
    pending(&t, e);

    fake.state()
        .fail("ResumeActor", [Status::deadline_exceeded("unreachable")]);
    let mut t = task("transient-resume", "img");
    let e = r.resume(&mut t, None, None).await.unwrap_err();
    pending(&t, e);
    r.resume(&mut t, None, None).await.unwrap();
    assert_eq!(
        fake.state().actor_state("default", "transient-resume"),
        Some(ateapi::ActorState::Running)
    );

    fake.state().fail(
        "CreateActor",
        [Status::invalid_argument("invalid actor specification")],
    );
    let mut t = task("permanent", "img");
    let e = r.suspend(&mut t, None, None).await.unwrap_err();
    assert!(!rax_server::is_transient(&e));
    assert_condition(&t, "Ready", "False", reason::ACTOR_CREATION_FAILED);
}

#[tokio::test]
async fn substrate_actors_report_state_and_worker() {
    let fake = FakeSubstrate::new();
    let actors = SubstrateActors::new(fake.start().await);
    assert_eq!(actors.get_actor("default", "none").await.unwrap(), None);
    fake.state()
        .put_actor("default", "a", ateapi::ActorState::Crashed);
    if let Some(st) = fake
        .state()
        .actors
        .get_mut("default/a")
        .and_then(|a| a.status.as_mut())
    {
        st.crash = Some(ateapi::ActorCrash {
            message: "oom".into(),
            ..Default::default()
        });
    }
    let a = actors.get_actor("default", "a").await.unwrap().unwrap();
    assert_eq!(
        (a.name.as_str(), a.state, a.crash_message.as_str()),
        ("a", ActorState::Crashed, "oom")
    );

    fake.state()
        .put_actor("default", "b", ateapi::ActorState::Suspended);
    let client = fake.start().await;
    client.resume_actor("default", "b").await.unwrap();
    let list = actors.list_actors("default").await.unwrap();
    assert_eq!(list.len(), 2);
    let b = list.iter().find(|a| a.name == "b").unwrap();
    assert_eq!(
        (b.state, b.worker_ip.as_str()),
        (ActorState::Running, "10.244.1.42")
    );

    fake.state()
        .fail("GetActor", [Status::permission_denied("no")]);
    assert_eq!(
        actors.get_actor("default", "a").await.unwrap_err().code(),
        Code::PermissionDenied
    );
}

#[tokio::test]
async fn the_service_drives_task_lifecycles_through_substrate() {
    use rax_api::v1alpha1::{
        CreateTaskRequest, DeleteTaskRequest, GetTaskRequest, ResumeTaskRequest, SuspendTaskRequest,
    };
    use rax_server::{AxService, ServerOptions};
    use std::sync::Arc;

    let fake = FakeSubstrate::new();
    let client = fake.start().await;
    let ready = ready_server().await;
    fake.state().worker_ip = ready.clone();
    let svc = AxService::new(
        Arc::new(rax_store::MemoryStore::new()),
        ServerOptions {
            reconciler: Some(Arc::new(TaskReconciler::new(
                client.clone(),
                ControllerOptions::default(),
            ))),
            actors: Some(Arc::new(SubstrateActors::new(client))),
            ..Default::default()
        },
    );
    let key = || ("default".to_owned(), "life".to_owned());

    let mut t = task("life", "img");
    t.metadata.as_mut().unwrap().atespace.clear();
    let created = svc
        .create_task(CreateTaskRequest { task: Some(t) })
        .await
        .unwrap();
    assert_eq!(status(&created).state(), TaskState::Suspended);
    assert_eq!(status(&created).attempt, 1);
    assert!(!created.metadata.as_ref().unwrap().uid.is_empty());
    assert_eq!(
        fake.state().actor_state("default", "life"),
        Some(ateapi::ActorState::Suspended)
    );

    let (atespace, name) = key();
    let resumed = svc
        .resume_task(ResumeTaskRequest { atespace, name })
        .await
        .unwrap();
    assert_eq!(status(&resumed).state(), TaskState::Working);
    let (atespace, name) = key();
    let got = svc
        .get_task(GetTaskRequest { atespace, name })
        .await
        .unwrap();
    assert_eq!(
        status(&got).state(),
        TaskState::Working,
        "read from the actor"
    );
    assert_eq!(status(&got).actor_uid, "uid-default/life");
    assert_condition(&got, "WorkspaceReady", "True", reason::SETUP_COMPLETE);

    let (atespace, name) = key();
    let suspended = svc
        .suspend_task(SuspendTaskRequest { atespace, name })
        .await
        .unwrap();
    assert_eq!(status(&suspended).state(), TaskState::Suspended);
    assert_eq!(
        fake.state().actor_state("default", "life"),
        Some(ateapi::ActorState::Suspended)
    );

    let (atespace, name) = key();
    svc.delete_task(DeleteTaskRequest { atespace, name })
        .await
        .unwrap();
    let s = fake.state();
    assert!(s.actors.is_empty());
    assert!(s.templates.is_empty());
}

fn gateway(host: &str) -> rax_api::v1alpha1::Gateway {
    use rax_api::substrate::ateapi::v1::{EgressRule, HttpsRule};
    rax_api::v1alpha1::Gateway {
        metadata: Some(ObjectMeta {
            name: "gw".into(),
            atespace: "default".into(),
            ..Default::default()
        }),
        spec: Some(rax_api::v1alpha1::GatewaySpec {
            egress: vec![EgressRule {
                https: Some(HttpsRule {
                    hostnames: vec![host.into()],
                    ..Default::default()
                }),
                ..Default::default()
            }],
        }),
        ..Default::default()
    }
}

/// The actor of a task that binds a gateway gets that gateway's rules as its
/// egress policy, once. For a task that binds none, the reconciler makes no
/// egress policy call.
#[tokio::test]
async fn gateway_rules_become_the_actor_egress_policy() {
    let (fake, r) = setup("").await;
    let mut unbound = task("unbound-task", "img");
    r.suspend(&mut unbound, None, None).await.unwrap();
    r.resume(&mut unbound, None, None).await.unwrap();
    let egress_calls = |s: &rax_substrate::testing::State| {
        s.calls_of("GetActorEgressPolicy").len() + s.calls_of("CreateActorEgressPolicy").len()
    };
    assert_eq!(egress_calls(&fake.state()), 0);

    let mut bound = task("bound-task", "img");
    bound.spec.as_mut().unwrap().gateway =
        Some(rax_api::v1alpha1::GatewayRef { name: "gw".into() });
    r.suspend(&mut bound, None, Some(gateway("a.example.com")))
        .await
        .unwrap();
    // A later binding with other rules keeps the policy the actor has.
    r.resume(&mut bound, None, Some(gateway("b.example.com")))
        .await
        .unwrap();
    {
        let s = fake.state();
        assert_eq!(
            s.calls_of("CreateActorEgressPolicy"),
            ["default/bound-task"]
        );
        let policy = &s.egress_policies["default/bound-task"];
        assert_eq!(policy.metadata.as_ref().unwrap().name, "default");
        assert_eq!(
            policy.rules[0].https.as_ref().unwrap().hostnames,
            ["a.example.com"]
        );
    }

    // A bound gateway that does not exist fails the binding.
    let mut missing = task("missing-gw", "img");
    missing.spec.as_mut().unwrap().gateway =
        Some(rax_api::v1alpha1::GatewayRef { name: "gw".into() });
    let err = r.suspend(&mut missing, None, None).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
    assert_condition(&missing, "Ready", "False", reason::GATEWAY_BINDING_FAILED);
}

/// A profile naming an existing ActorTemplate runs from it: no per-task
/// template is made, and deleting the task leaves the template.
#[tokio::test]
async fn ate_actor_templates_are_used_as_they_are() {
    let (fake, r) = setup("").await;
    fake.state()
        .templates
        .insert("default/custom-base".into(), template("custom-base"));
    let mut t = task("tpl-task", "");
    t.spec.as_mut().unwrap().profile = Some(Profile {
        ate_actor_template: "custom-base".into(),
        ..Default::default()
    });
    r.suspend(&mut t, None, None).await.unwrap();
    assert_eq!(status(&t).ate_actor_template, "custom-base");
    assert!(fake.state().created_templates.is_empty());
    r.delete(&t).await.unwrap();
    assert!(fake.state().templates.contains_key("default/custom-base"));
}

/// A task without a profile runs with its workspace's; a task profile with
/// any field set is used as it is, falling back to server defaults.
#[tokio::test]
async fn workspace_profiles_apply_whole() {
    use rax_api::v1alpha1::{Workspace, WorkspaceRef, WorkspaceSpec};
    const DEFAULT_IMG: &str = "ghcr.io/default/runner@sha256:1111";
    let (fake, r) = setup(DEFAULT_IMG).await;
    let limits = |cpu: &str, memory: &str| {
        Some(ResourcesSpec {
            limits: Some(ResourceList {
                cpu: cpu.into(),
                memory: memory.into(),
            }),
        })
    };
    let workspace = |name: &str, profile: Profile| Workspace {
        metadata: Some(ObjectMeta {
            name: name.into(),
            atespace: "default".into(),
            ..Default::default()
        }),
        spec: Some(WorkspaceSpec {
            profile: Some(profile),
            ..Default::default()
        }),
        ..Default::default()
    };
    let ws = workspace(
        "warm-ws",
        Profile {
            image: "ghcr.io/ws/image@sha256:2222".into(),
            resources: limits("4", "8Gi"),
            ..Default::default()
        },
    );
    let bound = |name: &str, ws: &str, profile: Option<Profile>| {
        let mut t = task(name, "");
        let spec = t.spec.as_mut().unwrap();
        spec.profile = profile;
        spec.workspace = Some(WorkspaceRef {
            name: ws.into(),
            ..Default::default()
        });
        t
    };

    // 1. A task without a profile inherits the workspace's.
    let mut inherited = bound("inherited-task", "warm-ws", None);
    r.suspend(&mut inherited, Some(ws.clone()), None)
        .await
        .unwrap();
    // 2. A task profile setting only resources uses server defaults for the
    // rest, not the workspace's.
    let mut resources_only = bound(
        "resources-only-task",
        "warm-ws",
        Some(Profile {
            resources: limits("", "2Gi"),
            ..Default::default()
        }),
    );
    r.suspend(&mut resources_only, Some(ws), None)
        .await
        .unwrap();
    // 3. The same over a workspace that names an ActorTemplate.
    fake.state()
        .templates
        .insert("default/ws-base-tpl".into(), template("ws-base-tpl"));
    let tpl_ws = workspace(
        "tpl-ws",
        Profile {
            ate_actor_template: "ws-base-tpl".into(),
            ..Default::default()
        },
    );
    let mut over_tpl = bound(
        "task-over-tpl-ws",
        "tpl-ws",
        Some(Profile {
            resources: limits("", "1Gi"),
            ..Default::default()
        }),
    );
    r.suspend(&mut over_tpl, Some(tpl_ws), None).await.unwrap();

    let s = fake.state();
    let got: Vec<_> = s
        .created_templates
        .iter()
        .map(|t| {
            let l = t.resources.as_ref().unwrap().limits.unwrap();
            (
                t.containers[0].image.as_str(),
                l.cpu_millicores,
                l.memory_bytes,
            )
        })
        .collect();
    assert_eq!(
        got,
        [
            ("ghcr.io/ws/image@sha256:2222", 4000, 8 << 30),
            (DEFAULT_IMG, DEFAULT_CPU_MILLICORES, 2 << 30),
            (DEFAULT_IMG, DEFAULT_CPU_MILLICORES, 1 << 30),
        ]
    );
}
