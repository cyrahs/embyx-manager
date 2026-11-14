//! The repository calls the service makes.
//!
//! Python origin: the `FillActorRepository` protocol of `fill_actor/persistence.py`, as far as
//! `service.py` uses it. [`PostgresFillActorRepository`] is the only implementation the app
//! has (D110); the trait is the seam the scenarios use to crash the service in front of a
//! repository call, as the Python scenario wraps the repository.

use crate::cloud_moves::BoxFuture;
use crate::models::MoveResult;
use crate::postgres_repository::{PostgresFillActorRepository, RepositoryError};
use crate::records::{CandidateRecord, CloudMoveOperationRecord, MoveJournalRecord, PlanRecord};

type Result<T> = std::result::Result<T, RepositoryError>;

/// `FillActorRepository`, the part the service calls.
pub trait FillActorRepository: Send + Sync {
    fn save_plan<'a>(&'a self, record: &'a PlanRecord) -> BoxFuture<'a, Result<()>>;
    fn get_plan<'a>(&'a self, plan_id: &'a str) -> BoxFuture<'a, Result<Option<PlanRecord>>>;
    fn get_candidate<'a>(
        &'a self,
        plan_id: &'a str,
        candidate_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<CandidateRecord>>>;
    fn save_move_result<'a>(&'a self, plan_id: &'a str, result: &'a MoveResult) -> BoxFuture<'a, Result<()>>;
    fn get_move_result<'a>(
        &'a self,
        plan_id: &'a str,
        candidate_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<MoveResult>>>;
    fn save_move_journal<'a>(&'a self, record: &'a MoveJournalRecord) -> BoxFuture<'a, Result<()>>;
    fn get_move_journal<'a>(
        &'a self,
        plan_id: &'a str,
        candidate_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<MoveJournalRecord>>>;
    fn list_unreconciled_moves(&self) -> BoxFuture<'_, Result<Vec<MoveJournalRecord>>>;
    fn save_cloud_move_operation<'a>(&'a self, record: &'a CloudMoveOperationRecord) -> BoxFuture<'a, Result<()>>;
    fn get_cloud_move_operation<'a>(
        &'a self,
        plan_id: &'a str,
        candidate_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<CloudMoveOperationRecord>>>;
    fn list_unresolved_cloud_moves(&self) -> BoxFuture<'_, Result<Vec<CloudMoveOperationRecord>>>;
    fn finalize_cloud_move<'a>(
        &'a self,
        operation: &'a CloudMoveOperationRecord,
        result: &'a MoveResult,
    ) -> BoxFuture<'a, Result<()>>;
}

impl FillActorRepository for PostgresFillActorRepository {
    fn save_plan<'a>(&'a self, record: &'a PlanRecord) -> BoxFuture<'a, Result<()>> {
        Box::pin(Self::save_plan(self, record))
    }

    fn get_plan<'a>(&'a self, plan_id: &'a str) -> BoxFuture<'a, Result<Option<PlanRecord>>> {
        Box::pin(Self::get_plan(self, plan_id))
    }

    fn get_candidate<'a>(
        &'a self,
        plan_id: &'a str,
        candidate_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<CandidateRecord>>> {
        Box::pin(Self::get_candidate(self, plan_id, candidate_id))
    }

    fn save_move_result<'a>(&'a self, plan_id: &'a str, result: &'a MoveResult) -> BoxFuture<'a, Result<()>> {
        Box::pin(Self::save_move_result(self, plan_id, result))
    }

    fn get_move_result<'a>(
        &'a self,
        plan_id: &'a str,
        candidate_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<MoveResult>>> {
        Box::pin(Self::get_move_result(self, plan_id, candidate_id))
    }

    fn save_move_journal<'a>(&'a self, record: &'a MoveJournalRecord) -> BoxFuture<'a, Result<()>> {
        Box::pin(Self::save_move_journal(self, record))
    }

    fn get_move_journal<'a>(
        &'a self,
        plan_id: &'a str,
        candidate_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<MoveJournalRecord>>> {
        Box::pin(Self::get_move_journal(self, plan_id, candidate_id))
    }

    fn list_unreconciled_moves(&self) -> BoxFuture<'_, Result<Vec<MoveJournalRecord>>> {
        Box::pin(Self::list_unreconciled_moves(self))
    }

    fn save_cloud_move_operation<'a>(&'a self, record: &'a CloudMoveOperationRecord) -> BoxFuture<'a, Result<()>> {
        Box::pin(Self::save_cloud_move_operation(self, record))
    }

    fn get_cloud_move_operation<'a>(
        &'a self,
        plan_id: &'a str,
        candidate_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<CloudMoveOperationRecord>>> {
        Box::pin(Self::get_cloud_move_operation(self, plan_id, candidate_id))
    }

    fn list_unresolved_cloud_moves(&self) -> BoxFuture<'_, Result<Vec<CloudMoveOperationRecord>>> {
        Box::pin(Self::list_unresolved_cloud_moves(self))
    }

    fn finalize_cloud_move<'a>(
        &'a self,
        operation: &'a CloudMoveOperationRecord,
        result: &'a MoveResult,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(Self::finalize_cloud_move(self, operation, result))
    }
}
