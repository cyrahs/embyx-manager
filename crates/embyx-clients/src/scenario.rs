//! The fakes of one effect-transcript scenario, started as `tools/scenarios/runner.py` starts
//! them: the CloudDrive fake and the two HTTP fakes (`kubernetes`, `emby`), all recording into
//! one [`Recorder`], each seeded from its section of the spec (empty when the spec has none).

use embyx_core::pyjson::{Dict, Value};
use embyx_core::scenario::{Recorder, Spec};

use crate::clouddrive::fake::{FakeCloudDrive, Served};
use crate::fakehttp::FakeHttp;

/// The running fakes.
#[derive(Debug)]
pub struct ScenarioFakes {
    pub clouddrive: Served,
    pub kubernetes: FakeHttp,
    pub emby: FakeHttp,
}

impl ScenarioFakes {
    pub async fn start(spec: &Spec, recorder: &Recorder) -> Self {
        let clouddrive = FakeCloudDrive::new(&spec.section("clouddrive"), recorder.clone())
            .serve()
            .await;
        let kubernetes = FakeHttp::start("kubernetes", &spec.section("kubernetes"), recorder.clone()).await;
        let emby = FakeHttp::start("emby", &spec.section("emby"), recorder.clone()).await;
        Self {
            clouddrive,
            kubernetes,
            emby,
        }
    }

    /// The transcript's `fakes`: each fake's final state.
    pub fn dump(&self) -> Value {
        let mut fakes = Dict::new();
        fakes.insert("clouddrive", self.clouddrive.fake.dump());
        fakes.insert("kubernetes", self.kubernetes.dump());
        fakes.insert("emby", self.emby.dump());
        Value::Dict(fakes)
    }
}
