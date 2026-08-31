use lark_codex_bridge::runtime::adoption::{
    ThreadAdoptionAvailability, ThreadAdoptionBackend, ThreadAdoptionError, ThreadAdoptionGate,
    ThreadAdoptionOperation, thread_adoption_platform_supported,
};
use serde_json::json;

#[test]
fn managed_process_backends_follow_the_platform_capability_gate() {
    let expected = if thread_adoption_platform_supported() {
        ThreadAdoptionAvailability::AvailableDedicatedProcessOwnership
    } else {
        ThreadAdoptionAvailability::UnavailablePlatformProcessTreeProof
    };
    for gate in [
        ThreadAdoptionGate::managed_stdio(),
        ThreadAdoptionGate::managed_sidecar(),
    ] {
        assert_eq!(gate.availability(), expected);
        assert_eq!(
            gate.availability().release_authority(),
            thread_adoption_platform_supported().then_some("dedicated_process_tree_reap")
        );
        for operation in [
            ThreadAdoptionOperation::Discover,
            ThreadAdoptionOperation::Adopt,
            ThreadAdoptionOperation::Release,
        ] {
            if thread_adoption_platform_supported() {
                assert_eq!(gate.require(operation), Ok(()));
            } else {
                assert_eq!(
                    gate.require(operation),
                    Err(ThreadAdoptionError::Unavailable(
                        ThreadAdoptionAvailability::UnavailablePlatformProcessTreeProof
                    ))
                );
            }
        }
    }
}

#[test]
fn shared_external_endpoint_fails_before_every_operation() {
    let gate = ThreadAdoptionGate::external_endpoint();
    assert_eq!(gate.backend(), ThreadAdoptionBackend::ExternalEndpoint);
    assert_eq!(
        gate.availability(),
        ThreadAdoptionAvailability::UnavailableSharedExternalEndpoint
    );

    for operation in [
        ThreadAdoptionOperation::Discover,
        ThreadAdoptionOperation::Adopt,
        ThreadAdoptionOperation::Release,
    ] {
        assert_eq!(
            gate.require(operation),
            Err(ThreadAdoptionError::Unavailable(
                ThreadAdoptionAvailability::UnavailableSharedExternalEndpoint
            ))
        );
    }
}

#[test]
fn availability_json_and_boolean_share_the_capability_classification() {
    let availability = ThreadAdoptionGate::managed_stdio().availability();

    assert_eq!(
        serde_json::to_value(availability).expect("availability should serialize"),
        json!(availability.code())
    );
    assert_eq!(
        availability.is_available(),
        thread_adoption_platform_supported()
    );
}

#[test]
fn unavailable_diagnostics_are_static_and_secret_free() {
    let error = ThreadAdoptionGate::external_endpoint()
        .require(ThreadAdoptionOperation::Adopt)
        .expect_err("shared endpoint adoption must remain disabled");
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
    assert!(display.contains("unavailable"));
    assert!(display.contains("issue #8"));
}
