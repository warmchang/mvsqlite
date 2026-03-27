use std::{cell::RefCell, sync::Arc};

use anyhow::{bail, Context, Result};
use mvclient::{CommitResult, MultiVersionClient, NamespaceCommitIntent};
use reqwest::Url;

pub struct CommitGroup {
    intents: Vec<NamespaceCommitIntent>,
    client: Option<Arc<MultiVersionClient>>,
    dp: Option<Url>,
    pub current_version: Option<String>,
}

impl Default for CommitGroup {
    fn default() -> Self {
        Self {
            intents: Vec::new(),
            client: None,
            dp: None,
            current_version: None,
        }
    }
}

pub enum TransactionStart {
    Normal,
    UseVersion(String),
    Reject,
}

pub enum CommitOutput {
    Empty,
    Committed(CommitResult),
    Conflict,
}

thread_local! {
    static CURRENT_COMMIT_GROUP: RefCell<Option<CommitGroup>> = RefCell::new(None);
}

pub fn begin() -> Result<()> {
    CURRENT_COMMIT_GROUP.with(|cg| {
        let mut cg = cg.borrow_mut();
        if cg.is_some() {
            bail!("mv_commit_group_begin called recursively in a commit group");
        }
        *cg = Some(CommitGroup::default());
        Ok(())
    })
}

pub fn is_active() -> bool {
    CURRENT_COMMIT_GROUP.with(|cg| cg.borrow().is_some())
}

pub fn transaction_start() -> TransactionStart {
    CURRENT_COMMIT_GROUP.with(|cg| {
        let cg = cg.borrow();
        match &*cg {
            Some(cg) if !cg.intents.is_empty() => TransactionStart::Reject,
            Some(cg) => match &cg.current_version {
                Some(version) => TransactionStart::UseVersion(version.clone()),
                None => TransactionStart::Normal,
            },
            None => TransactionStart::Normal,
        }
    })
}

pub fn set_current_version(version: &str) {
    CURRENT_COMMIT_GROUP.with(|cg| {
        if let Some(cg) = &mut *cg.borrow_mut() {
            cg.current_version.get_or_insert_with(|| version.to_string());
        }
    });
}

pub fn append_intent(
    client: &Arc<MultiVersionClient>,
    dp: Option<&Url>,
    intent: NamespaceCommitIntent,
) -> Result<()> {
    CURRENT_COMMIT_GROUP.with(|cg| {
        let mut cg = cg.borrow_mut();
        let cg = cg
            .as_mut()
            .context("transaction commit attempted without a commit group open")?;

        if cg.client.is_none() {
            cg.client = Some(client.clone());
        }
        if cg.dp.is_none() {
            cg.dp = dp.cloned();
        }

        cg.intents.push(intent);
        Ok(())
    })
}

pub async fn commit() -> Result<CommitOutput> {
    let cg = CURRENT_COMMIT_GROUP.with(|cg| cg.borrow_mut().take());
    let mut cg = cg.context("mv_commit_group_commit called without a commit group open")?;

    if cg.intents.is_empty() {
        return Ok(CommitOutput::Empty);
    }

    let client = cg
        .client
        .take()
        .context("mv_commit_group_commit called without a client")?;
    let result = client.apply_commit_intents(cg.dp.as_ref(), &cg.intents).await?;

    Ok(match result {
        Some(result) => CommitOutput::Committed(result),
        None => CommitOutput::Conflict,
    })
}

pub async fn rollback() -> Result<()> {
    let cg = CURRENT_COMMIT_GROUP.with(|cg| cg.borrow_mut().take());
    let mut cg = cg.context("mv_commit_group_rollback called without a commit group open")?;

    if cg.intents.is_empty() {
        return Ok(());
    }

    for intent in &mut cg.intents {
        intent.init.num_pages = 0;
        intent.requests.clear();
    }

    let client = cg
        .client
        .take()
        .context("mv_commit_group_rollback called without a client")?;
    client.apply_commit_intents(cg.dp.as_ref(), &cg.intents).await?;
    Ok(())
}
