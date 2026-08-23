use lark_codex_bridge::runtime::adoption::{
    ThreadAdoptionAvailability, ThreadAdoptionError, ThreadAdoptionGate, ThreadAdoptionOperation,
};

#[test]
fn every_persisted_thread_operation_fails_at_the_zero_state_gate() {
    let gate = ThreadAdoptionGate;
    assert_eq!(
        gate.availability(),
        ThreadAdoptionAvailability::UnavailableNoReliableWriterRelease
    );

    for operation in [
        ThreadAdoptionOperation::Discover,
        ThreadAdoptionOperation::Adopt,
        ThreadAdoptionOperation::Release,
    ] {
        assert_eq!(
            gate.require(operation),
            Err(ThreadAdoptionError::Unavailable(
                ThreadAdoptionAvailability::UnavailableNoReliableWriterRelease
            ))
        );
    }
}

#[test]
fn unavailable_diagnostics_are_static_and_secret_free() {
    let error = ThreadAdoptionGate
        .require(ThreadAdoptionOperation::Adopt)
        .expect_err("adoption must remain disabled");
    let display = error.to_string();
    let debug = format!("{error:?}");

    for secret in [
        "thread-sensitive-id",
        "/sensitive/customer/workspace",
        "active writer owner",
    ] {
        assert!(!display.contains(secret));
        assert!(!debug.contains(secret));
    }
    assert!(display.contains("disabled"));
    assert!(display.contains("issue #8"));
}
