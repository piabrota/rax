//! Table rows and `describe` output for tasks and workspaces, as AX's CLI
//! prints them.

use std::fmt::Write as _;

use rax_api::format_task_state;
use rax_api::v1alpha1::{
    McpConfig, Profile, Task, TaskResult, TaskStatusReason, Workspace, WorkspaceSetup,
};

use crate::commands::{age_of, struct_json};
use crate::format::{go_strings, sorted, table};

fn or_none(v: &str) -> String {
    if v.is_empty() {
        "<none>".into()
    } else {
        v.into()
    }
}

fn name_and_atespace<M>(m: &Option<M>, f: impl Fn(&M) -> (&str, &str)) -> (String, String) {
    m.as_ref().map_or_else(Default::default, |m| {
        let (n, a) = f(m);
        (n.into(), a.into())
    })
}

/// A `get tasks` row: NAME, ATESPACE, STATE, ACTOR, AGE.
pub fn task_row(t: &Task) -> Vec<String> {
    let (name, atespace) = name_and_atespace(&t.metadata, |m| (&m.name, &m.atespace));
    let actor = t.status.as_ref().map_or("", |s| s.actor.as_str());
    vec![
        name,
        atespace,
        format_task_state(t).into(),
        or_none(actor),
        age_of(&t.metadata),
    ]
}

/// A `get workspaces` row: NAME, ATESPACE, GIT-REPOS, MCP-SERVERS.
pub fn workspace_row(ws: &Workspace) -> Vec<String> {
    let (name, atespace) = name_and_atespace(&ws.metadata, |m| (&m.name, &m.atespace));
    let spec = ws.spec.clone().unwrap_or_default();
    vec![
        name,
        atespace,
        spec.setup.map_or(0, |s| s.git.len()).to_string(),
        spec.mcp.map_or(0, |m| m.servers.len()).to_string(),
    ]
}

fn conditions(out: &mut String, conds: &[rax_api::v1alpha1::Condition]) {
    if conds.is_empty() {
        return;
    }
    out.push_str("\nConditions:\n");
    let mut rows = vec![vec![
        "  TYPE".to_owned(),
        "STATUS".into(),
        "REASON".into(),
        "MESSAGE".into(),
    ]];
    for c in conds {
        rows.push(vec![
            format!("  {}", c.r#type),
            c.status.clone(),
            c.reason.clone(),
            c.message.clone(),
        ]);
    }
    out.push_str(&table(&rows, 2));
}

/// `describe task` output.
pub fn describe_task(task: &Task) -> String {
    let mut out = String::new();
    let meta = task.metadata.clone().unwrap_or_default();
    let st = task.status.clone().unwrap_or_default();
    let _ = writeln!(out, "Name:         {}", meta.name);
    let _ = writeln!(out, "Atespace:     {}", meta.atespace);
    if !meta.uid.is_empty() {
        let _ = writeln!(out, "UID:          {}", meta.uid);
    }
    let _ = writeln!(out, "State:        {}", format_task_state(task));
    if let Some(d) = &st.status_detail
        && (d.reason() != TaskStatusReason::Unspecified || !d.detail.is_empty())
    {
        let _ = writeln!(
            out,
            "Detail:       {} ({})",
            d.reason().as_str_name(),
            d.detail
        );
    }
    let _ = writeln!(out, "Actor:        {}", st.actor);
    let _ = writeln!(out, "Actor UID:    {}", or_none(&st.actor_uid));
    if !st.ate_actor_template.is_empty() {
        let _ = writeln!(out, "Template:     {}", st.ate_actor_template);
    }
    if st.attempt > 0 {
        let _ = writeln!(out, "Attempt:      {}", st.attempt);
    }

    if let Some(spec) = &task.spec {
        if !spec.session_id.is_empty() {
            let _ = writeln!(out, "Session ID:   {}", spec.session_id);
        }
        if let Some(r) = &spec.workspace {
            let _ = writeln!(
                out,
                "Workspace:    {}  path={}",
                r.name,
                spec.workspace_path()
            );
        }
        if let Some(gw) = spec.gateway.as_ref().filter(|g| !g.name.is_empty()) {
            let _ = writeln!(out, "Gateway:      {}", gw.name);
        }
        describe_profile(&mut out, spec.profile.as_ref());
        if !spec.commands.is_empty() {
            let _ = writeln!(out, "Commands:     {}", go_strings(&spec.commands));
        }
        if let Some(goal) = spec.goal.as_ref().filter(|g| !g.spec.is_empty()) {
            let _ = writeln!(out, "Goal:         {}", goal.spec);
        }
        let config = struct_json(&spec.config);
        if !config.is_empty()
            && let Ok(yaml) = serde_yaml_ng::to_string(&sorted(&serde_json::Value::Object(config)))
        {
            out.push_str("Config:\n");
            for line in yaml.trim_end_matches('\n').lines() {
                let _ = writeln!(out, "  {line}");
            }
        }
    }

    if let Some(o) = &st.outcome {
        let result = TaskResult::try_from(o.result).unwrap_or_default();
        let _ = writeln!(
            out,
            "Outcome:      {} (exitCode={})",
            result.as_str_name(),
            o.exit_code
        );
        if !o.output.is_empty() {
            out.push_str("Output:\n");
            for line in o.output.trim_end_matches('\n').split('\n') {
                let _ = writeln!(out, "  {line}");
            }
        }
    }
    conditions(&mut out, &st.conditions);
    out
}

/// `describe workspace` output.
pub fn describe_workspace(ws: &Workspace) -> String {
    let mut out = String::new();
    let meta = ws.metadata.clone().unwrap_or_default();
    let st = ws.status.clone().unwrap_or_default();
    let spec = ws.spec.clone().unwrap_or_default();
    let _ = writeln!(out, "Name:         {}", meta.name);
    let _ = writeln!(out, "Atespace:     {}", meta.atespace);
    if !st.phase.is_empty() {
        let _ = writeln!(out, "Phase:        {}", st.phase);
    }
    if !st.last_setup_error.is_empty() {
        let _ = writeln!(out, "Last Error:   {}", st.last_setup_error);
    }
    describe_profile(&mut out, spec.profile.as_ref());
    describe_setup(&mut out, spec.setup.as_ref());
    describe_mcp(&mut out, spec.mcp.as_ref());
    conditions(&mut out, &st.conditions);
    out
}

fn describe_profile(out: &mut String, p: Option<&Profile>) {
    let Some(p) = p else { return };
    if !p.image.is_empty() {
        let _ = writeln!(out, "Image:        {}", p.image);
    }
    if let Some(l) = p.resources.as_ref().and_then(|r| r.limits.as_ref())
        && (!l.cpu.is_empty() || !l.memory.is_empty())
    {
        let _ = writeln!(out, "Resources:    cpu={} memory={}", l.cpu, l.memory);
    }
    if !p.ate_actor_template.is_empty() {
        let _ = writeln!(out, "ATE Template: {}", p.ate_actor_template);
    }
}

fn describe_setup(out: &mut String, setup: Option<&WorkspaceSetup>) {
    let Some(setup) = setup else { return };
    if !setup.git.is_empty() {
        out.push_str("Git Repositories:\n");
        for g in &setup.git {
            let git_ref = if g.r#ref.is_empty() { "main" } else { &g.r#ref };
            let label = if g.name.is_empty() { &g.repo } else { &g.name };
            let _ = writeln!(out, "  - {label} ({}, ref: {git_ref})", g.repo);
        }
    }
    if !setup.files.is_empty() {
        out.push_str("Setup Files:\n");
        for f in &setup.files {
            let _ = writeln!(out, "  - {}", f.path);
        }
    }
    if !setup.commands.is_empty() {
        out.push_str("Setup Commands:\n");
        for c in &setup.commands {
            let _ = writeln!(out, "  - {c}");
        }
    }
    if let Some(skills) = &setup.skills {
        if !skills.registries.is_empty() {
            out.push_str("Skill Registries:\n");
            for r in &skills.registries {
                let _ = writeln!(out, "  - Provider: {}, Query: {:?}", r.provider, r.query);
            }
        }
        if !skills.path.is_empty() {
            let _ = writeln!(out, "Skills Path:  {}", skills.path);
        }
    }
    if let Some(gw) = setup.gateway.as_ref().filter(|g| !g.name.is_empty()) {
        let _ = writeln!(out, "Setup Gateway: {}", gw.name);
    }
}

fn describe_mcp(out: &mut String, mcp: Option<&McpConfig>) {
    let Some(mcp) = mcp else { return };
    if !mcp.registries.is_empty() {
        out.push_str("MCP Registries:\n");
        for r in &mcp.registries {
            let _ = writeln!(out, "  - Provider: {}, Query: {:?}", r.provider, r.query);
        }
    }
    if !mcp.servers.is_empty() {
        out.push_str("MCP Servers:\n");
        for s in &mcp.servers {
            if !s.endpoint.is_empty() {
                let _ = writeln!(out, "  - {}: {}", s.name, s.endpoint);
            } else {
                let _ = writeln!(out, "  - {}: {} {}", s.name, s.command, go_strings(&s.args));
            }
        }
    }
}
