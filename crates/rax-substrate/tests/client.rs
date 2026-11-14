//! The client against the in-memory Substrate server.

use std::collections::BTreeMap;

use rax_substrate::ateapi::ActorState;
use rax_substrate::testing::{DEFAULT_WORKER_IP, FakeSubstrate};
use rax_substrate::{RetryPolicy, TokenInterceptor};
use tonic::{Code, Status};

#[tokio::test]
async fn ensure_atespace_tolerates_existing_and_retries_unavailable() {
    let fake = FakeSubstrate::new();
    let client = fake.start().await;
    fake.state()
        .fail("CreateAtespace", [Status::unavailable("down")]);
    client.ensure_atespace("team").await.unwrap();
    client.ensure_atespace("team").await.unwrap();
    assert_eq!(
        fake.state().calls_of("CreateAtespace"),
        ["team", "team", "team"]
    );

    fake.state()
        .fail("CreateAtespace", [Status::permission_denied("no")]);
    let err = client.ensure_atespace("other").await.unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);
    assert_eq!(err.to_string(), "creating atespace \"other\": no");
}

#[tokio::test]
async fn ensure_actor_template_creates_once() {
    let fake = FakeSubstrate::new();
    let client = fake.start().await;
    let env = BTreeMap::from([("A".to_owned(), "1".to_owned())]);
    let tmpl = client
        .ensure_actor_template("default", "t-tmpl-1", "img:1", None, &env)
        .await
        .unwrap();
    assert_eq!(tmpl.containers[0].image, "img:1");
    // An existing template is returned as it is, whatever the arguments.
    let again = client
        .ensure_actor_template("default", "t-tmpl-1", "img:2", None, &env)
        .await
        .unwrap();
    assert_eq!(again.containers[0].image, "img:1");
    assert_eq!(fake.state().created_templates.len(), 1);
}

#[tokio::test]
async fn ensure_actor_template_propagates_errors() {
    let fake = FakeSubstrate::new();
    let client = fake.start().await;
    fake.state().fail(
        "CreateActorTemplate",
        [Status::invalid_argument("bad quantity")],
    );
    let err = client
        .ensure_actor_template("default", "t", "", None, &BTreeMap::new())
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert_eq!(err.message(), "bad quantity");
    assert_eq!(
        err.to_string(),
        "creating actor template default/t: bad quantity"
    );
}

#[tokio::test]
async fn ensure_actor_returns_existing_and_reverts_crashed() {
    let fake = FakeSubstrate::new();
    let client = fake.start().await;
    let a = client
        .ensure_actor("default", "t", "ax-system", "tmpl")
        .await
        .unwrap();
    assert_eq!(a.actor_template.unwrap().name, "tmpl");
    client
        .ensure_actor("default", "t", "ax-system", "tmpl")
        .await
        .unwrap();
    assert_eq!(fake.state().calls_of("CreateActor").len(), 2);
    assert!(fake.state().calls_of("RevertActor").is_empty());

    fake.state().put_actor("default", "t", ActorState::Crashed);
    let a = client
        .ensure_actor("default", "t", "ax-system", "tmpl")
        .await
        .unwrap();
    assert_eq!(a.status.unwrap().state, ActorState::Suspended as i32);
    assert_eq!(fake.state().calls_of("RevertActor"), ["default/t"]);
    assert!(
        fake.state().calls_of("DeleteActor").is_empty(),
        "a crashed actor is reverted, not deleted"
    );
}

#[tokio::test]
async fn ensure_actor_recreates_crashed_actor_that_cannot_be_reverted() {
    let fake = FakeSubstrate::new();
    let client = fake.start().await;
    fake.state().put_actor("default", "t", ActorState::Crashed);
    fake.state()
        .fail("RevertActor", [Status::failed_precondition("no snapshot")]);
    client
        .ensure_actor("default", "t", "ax-system", "tmpl")
        .await
        .unwrap();
    let s = fake.state();
    assert_eq!(s.calls_of("DeleteActor"), ["default/t"]);
    assert_eq!(s.calls_of("CreateActor").len(), 2);
    assert_eq!(s.actor_state("default", "t"), Some(ActorState::Suspended));
}

#[tokio::test]
async fn ensure_actor_waits_for_a_deleting_actor() {
    let fake = FakeSubstrate::new();
    let client = fake.start().await;
    fake.state().put_actor("default", "t", ActorState::Deleting);
    // The old actor goes away after two more calls look at it.
    fake.state().deleting.insert("default/t".into(), 2);
    let a = client
        .ensure_actor("default", "t", "ax-system", "tmpl")
        .await
        .unwrap();
    assert_eq!(a.status.unwrap().state, ActorState::Suspended as i32);
    assert_eq!(fake.state().calls_of("CreateActor").len(), 2);
}

#[tokio::test]
async fn ensure_actor_gives_up_on_an_actor_that_stays_deleting() {
    let fake = FakeSubstrate::new();
    let client = fake.start().await.with_retry_policy(RetryPolicy {
        deleting_wait_attempts: 3,
        ..RetryPolicy::fast()
    });
    fake.state().put_actor("default", "t", ActorState::Deleting);
    fake.state().deleting.insert("default/t".into(), usize::MAX);
    let err = client
        .ensure_actor("default", "t", "ax-system", "tmpl")
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "actor default/t is still deleting; please retry"
    );
    assert_eq!(fake.state().calls_of("CreateActor").len(), 4);
}

#[tokio::test]
async fn resume_returns_worker_ip_and_retries_aborted() {
    let fake = FakeSubstrate::new();
    let client = fake.start().await;
    client
        .ensure_actor("default", "t", "ax-system", "tmpl")
        .await
        .unwrap();
    fake.state().fail(
        "ResumeActor",
        [Status::aborted("busy"), Status::unavailable("down")],
    );
    let (actor, ip) = client.resume_actor("default", "t").await.unwrap();
    assert_eq!(ip, DEFAULT_WORKER_IP);
    assert_eq!(rax_substrate::worker_ip(&actor), DEFAULT_WORKER_IP);
    assert_eq!(fake.state().calls_of("ResumeActor").len(), 3);

    fake.state()
        .fail("ResumeActor", [Status::resource_exhausted("no workers")]);
    let err = client.resume_actor("default", "t").await.unwrap_err();
    assert_eq!(err.code(), Code::ResourceExhausted);
    assert_eq!(
        fake.state().calls_of("ResumeActor").len(),
        4,
        "ResourceExhausted is not retried"
    );

    client.suspend_actor("default", "t").await.unwrap();
    let actor = client.get_actor("default", "t").await.unwrap();
    assert_eq!(rax_substrate::worker_ip(&actor), "");
}

#[tokio::test]
async fn delete_actor_waits_until_gone() {
    let fake = FakeSubstrate::new();
    let client = fake.start().await;
    client
        .ensure_actor("default", "t", "ax-system", "tmpl")
        .await
        .unwrap();
    fake.state().deleting_polls = 3;
    client.delete_actor("default", "t").await.unwrap();
    let s = fake.state();
    assert_eq!(
        s.calls_of("GetActor").len(),
        4,
        "three polls see DELETING, the fourth NotFound"
    );
    assert!(s.actors.is_empty());
}

#[tokio::test]
async fn delete_actor_ignores_missing_and_times_out() {
    let fake = FakeSubstrate::new();
    let client = fake.start().await;
    client.delete_actor("default", "missing").await.unwrap();
    assert!(fake.state().calls_of("GetActor").is_empty());

    let client = client.with_retry_policy(RetryPolicy {
        delete_timeout: std::time::Duration::from_millis(50),
        ..RetryPolicy::fast()
    });
    client
        .ensure_actor("default", "t", "ax-system", "tmpl")
        .await
        .unwrap();
    fake.state().deleting_polls = usize::MAX;
    let err = client.delete_actor("default", "t").await.unwrap_err();
    assert_eq!(err.code(), Code::DeadlineExceeded);
    assert!(
        err.to_string()
            .starts_with("waiting for actor default/t deletion"),
        "{err}"
    );
}

#[tokio::test]
async fn delete_actor_template_retries_aborted_and_ignores_missing() {
    let fake = FakeSubstrate::new();
    let client = fake.start().await;
    client
        .ensure_actor_template("default", "t", "", None, &BTreeMap::new())
        .await
        .unwrap();
    fake.state()
        .fail("DeleteActorTemplate", [Status::aborted("conflict")]);
    client.delete_actor_template("default", "t").await.unwrap();
    client.delete_actor_template("default", "t").await.unwrap();
    assert_eq!(fake.state().calls_of("DeleteActorTemplate").len(), 3);
    assert!(fake.state().templates.is_empty());
}

#[tokio::test]
async fn lists_paginate() {
    let fake = FakeSubstrate::new();
    let client = fake.start().await;
    for name in ["a", "b", "c", "d", "e"] {
        client
            .ensure_actor("team", name, "ax-system", "tmpl")
            .await
            .unwrap();
    }
    client
        .ensure_actor("other", "x", "ax-system", "tmpl")
        .await
        .unwrap();
    fake.state().list_page_size = 2;
    let names: Vec<_> = client
        .list_actors("team")
        .await
        .unwrap()
        .into_iter()
        .map(|a| a.metadata.unwrap().name)
        .collect();
    assert_eq!(names, ["a", "b", "c", "d", "e"]);
    assert_eq!(fake.state().calls_of("ListActors").len(), 3);
    assert_eq!(client.list_actors("").await.unwrap().len(), 6);
}

#[tokio::test]
async fn sends_the_token_from_the_token_file() {
    let dir = std::env::temp_dir().join(format!("rax-token-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let token = dir.join("token");
    std::fs::write(&token, "secret-1\n").unwrap();
    let fake = FakeSubstrate::new();
    let client = fake
        .start_with_auth(TokenInterceptor::from_file(&token))
        .await;
    client.ensure_atespace("a").await.unwrap();
    assert_eq!(
        fake.state().last_authorization.as_deref(),
        Some("Bearer secret-1")
    );
    // A rotated token is picked up on the next call.
    std::fs::write(&token, "secret-2").unwrap();
    client.ensure_atespace("b").await.unwrap();
    assert_eq!(
        fake.state().last_authorization.as_deref(),
        Some("Bearer secret-2")
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn ensure_template_returns_lookup_errors_other_than_not_found() {
    let fake = FakeSubstrate::new();
    let client = fake.start().await;
    fake.state()
        .fail("GetActorTemplate", [Status::permission_denied("no")]);
    let err = client
        .ensure_actor_template("default", "t", "", None, &BTreeMap::new())
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);
    assert!(fake.state().created_templates.is_empty());
}

#[tokio::test]
async fn egress_policy_is_created_once() {
    use rax_substrate::ateapi::{EgressRule, HttpsRule};
    let fake = FakeSubstrate::new();
    let client = fake.start().await;
    fake.state()
        .put_actor("default", "a", ActorState::Suspended);
    let err = client
        .get_actor_egress_policy("default", "a")
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    let rules = [EgressRule {
        https: Some(HttpsRule {
            hostnames: vec!["api.example.com".into()],
            ..Default::default()
        }),
        ..Default::default()
    }];
    client
        .ensure_actor_egress_policy("default", "a", &rules)
        .await
        .unwrap();
    // A second create finds the policy and keeps it.
    client
        .ensure_actor_egress_policy("default", "a", &[])
        .await
        .unwrap();
    let policy = client
        .get_actor_egress_policy("default", "a")
        .await
        .unwrap();
    assert_eq!(policy.rules, rules);
    assert_eq!(policy.metadata.unwrap().name, "default");

    fake.state().fail(
        "CreateActorEgressPolicy",
        [Status::invalid_argument("bad rule")],
    );
    fake.state()
        .put_actor("default", "b", ActorState::Suspended);
    let err = client
        .ensure_actor_egress_policy("default", "b", &rules)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn tags_seed_actors() {
    let fake = FakeSubstrate::new();
    let client = fake.start().await;
    client.ensure_atespace("default").await.unwrap();
    client
        .ensure_actor_template("default", "tmpl", "img", None, &BTreeMap::new())
        .await
        .unwrap();
    client
        .ensure_actor("default", "setup", "default", "tmpl")
        .await
        .unwrap();
    let tag = client
        .create_tag("default", "ws-tag", "setup")
        .await
        .unwrap();
    assert_eq!(tag.status.unwrap().actor_template_uid, "uid-default/tmpl");
    assert_eq!(client.get_tag("default", "ws-tag").await.unwrap().scope, 1);
    assert_eq!(client.list_tags("default").await.unwrap().len(), 1);
    assert!(client.list_tags("other").await.unwrap().is_empty());

    let actor = client
        .ensure_actor_from_tag("default", "task", "default", "tmpl", "ws-tag")
        .await
        .unwrap();
    assert_eq!(actor.source_tag.unwrap().name, "ws-tag");
    let err = client
        .ensure_actor_from_tag("default", "task2", "default", "tmpl", "missing")
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);

    client.delete_tag("default", "ws-tag").await.unwrap();
    client.delete_tag("default", "ws-tag").await.unwrap();
    assert_eq!(
        client
            .get_tag("default", "ws-tag")
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
}

#[tokio::test]
async fn ensure_actor_recreates_an_actor_deleted_after_already_exists() {
    let fake = FakeSubstrate::new();
    let client = fake.start().await;
    fake.state().put_actor("default", "a", ActorState::Deleting);
    // The create sees the old actor, which is gone by the time it is read.
    fake.state().fail("GetActor", [Status::not_found("gone")]);
    fake.state().actors.clear();
    fake.state()
        .fail("CreateActor", [Status::already_exists("still there")]);
    let actor = client
        .ensure_actor("default", "a", "default", "tmpl")
        .await
        .unwrap();
    assert_eq!(actor.metadata.unwrap().uid, "uid-default/a");
    assert_eq!(fake.state().calls_of("CreateActor").len(), 2);
}
