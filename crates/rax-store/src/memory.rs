//! An in-process store for tests and local runs. It orders lists like the
//! PostgreSQL store: most recently saved first.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use rax_api::v1alpha1::{Gateway, Model, Task, TaskStatus, Workspace};

use crate::{
    DEFAULT_TASK_LIST_LIMIT, ResourceStore, Result, StoreError, TaskStore, all_atespaces,
    default_atespace, meta_key, prepare_gateway, prepare_model, prepare_task, prepare_workspace,
};

type Key = (String, String);

/// One kind of resource, each stored with the sequence number of its last
/// save.
struct Table<T> {
    rows: HashMap<Key, (u64, T)>,
}

impl<T> Default for Table<T> {
    fn default() -> Self {
        Self {
            rows: HashMap::new(),
        }
    }
}

impl<T: Clone> Table<T> {
    fn get(&self, atespace: &str, name: &str) -> Result<T> {
        self.rows
            .get(&(default_atespace(atespace).to_owned(), name.to_owned()))
            .map(|(_, v)| v.clone())
            .ok_or(StoreError::NotFound)
    }

    fn list(&self, atespace: &str, limit: i64, offset: i64) -> Vec<T> {
        let mut rows: Vec<_> = self
            .rows
            .iter()
            .filter(|((a, _), _)| all_atespaces(atespace) || a == atespace)
            .collect();
        rows.sort_by(|(ka, (sa, _)), (kb, (sb, _))| sb.cmp(sa).then_with(|| ka.cmp(kb)));
        let rows = rows
            .into_iter()
            .skip(offset.max(0) as usize)
            .map(|(_, (_, v))| v.clone());
        if limit > 0 {
            rows.take(limit as usize).collect()
        } else {
            rows.collect()
        }
    }

    fn delete(&mut self, atespace: &str, name: &str) {
        self.rows
            .remove(&(default_atespace(atespace).to_owned(), name.to_owned()));
    }
}

#[derive(Default)]
struct State {
    seq: u64,
    tasks: Table<Task>,
    workspaces: Table<Workspace>,
    gateways: Table<Gateway>,
    models: Table<Model>,
}

impl State {
    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }
}

/// An in-memory store. Clones of a resource are handed out, so callers never
/// share state with the store.
#[derive(Default)]
pub struct MemoryStore {
    state: Mutex<State>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn with<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        f(&mut self.state.lock().unwrap())
    }
}

#[async_trait]
impl TaskStore for MemoryStore {
    async fn save_task(&self, task: &mut Task) -> Result<()> {
        prepare_task(task)?;
        let key = meta_key(&task.metadata);
        self.with(|s| {
            let seq = s.next_seq();
            s.tasks.rows.insert(key, (seq, task.clone()));
        });
        Ok(())
    }

    async fn get_task(&self, atespace: &str, name: &str) -> Result<Task> {
        self.with(|s| s.tasks.get(atespace, name))
    }

    async fn list_tasks(&self, atespace: &str, limit: i64, offset: i64) -> Result<Vec<Task>> {
        let limit = if limit <= 0 {
            DEFAULT_TASK_LIST_LIMIT
        } else {
            limit
        };
        Ok(self.with(|s| s.tasks.list(atespace, limit, offset)))
    }

    async fn update_task_status(
        &self,
        atespace: &str,
        name: &str,
        status: TaskStatus,
    ) -> Result<()> {
        self.with(|s| {
            let key = (default_atespace(atespace).to_owned(), name.to_owned());
            let (_, task) = s.tasks.rows.get_mut(&key).ok_or(StoreError::NotFound)?;
            task.status = Some(status);
            Ok(())
        })
    }

    async fn delete_task(&self, atespace: &str, name: &str) -> Result<()> {
        self.with(|s| s.tasks.delete(atespace, name));
        Ok(())
    }
}

#[async_trait]
impl ResourceStore for MemoryStore {
    async fn save_workspace(&self, ws: &mut Workspace) -> Result<()> {
        prepare_workspace(ws)?;
        let key = meta_key(&ws.metadata);
        self.with(|s| {
            let seq = s.next_seq();
            s.workspaces.rows.insert(key, (seq, ws.clone()));
        });
        Ok(())
    }

    async fn get_workspace(&self, atespace: &str, name: &str) -> Result<Workspace> {
        self.with(|s| s.workspaces.get(atespace, name))
    }

    async fn list_workspaces(&self, atespace: &str) -> Result<Vec<Workspace>> {
        Ok(self.with(|s| s.workspaces.list(atespace, 0, 0)))
    }

    async fn delete_workspace(&self, atespace: &str, name: &str) -> Result<()> {
        self.with(|s| s.workspaces.delete(atespace, name));
        Ok(())
    }

    async fn save_gateway(&self, gw: &mut Gateway) -> Result<()> {
        prepare_gateway(gw)?;
        let key = meta_key(&gw.metadata);
        self.with(|s| {
            let seq = s.next_seq();
            s.gateways.rows.insert(key, (seq, gw.clone()));
        });
        Ok(())
    }

    async fn get_gateway(&self, atespace: &str, name: &str) -> Result<Gateway> {
        self.with(|s| s.gateways.get(atespace, name))
    }

    async fn list_gateways(&self, atespace: &str) -> Result<Vec<Gateway>> {
        Ok(self.with(|s| s.gateways.list(atespace, 0, 0)))
    }

    async fn delete_gateway(&self, atespace: &str, name: &str) -> Result<()> {
        self.with(|s| s.gateways.delete(atespace, name));
        Ok(())
    }

    async fn save_model(&self, model: &mut Model) -> Result<()> {
        prepare_model(model)?;
        let key = meta_key(&model.metadata);
        self.with(|s| {
            let seq = s.next_seq();
            s.models.rows.insert(key, (seq, model.clone()));
        });
        Ok(())
    }

    async fn get_model(&self, atespace: &str, name: &str) -> Result<Model> {
        self.with(|s| s.models.get(atespace, name))
    }

    async fn list_models(&self, atespace: &str) -> Result<Vec<Model>> {
        Ok(self.with(|s| s.models.list(atespace, 0, 0)))
    }

    async fn delete_model(&self, atespace: &str, name: &str) -> Result<()> {
        self.with(|s| s.models.delete(atespace, name));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn conformance() {
        crate::testing::conformance(&super::MemoryStore::new()).await;
    }
}
