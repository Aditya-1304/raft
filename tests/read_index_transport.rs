use raft::{
    core::read_index::MAX_READ_INDEX_CONTEXT_BYTES,
    message::{Envelope, Message, ReadIndexRequest, ReadIndexResponse, TimeoutNowRequest},
    runtime::transport_tcp::TcpEnvelopeCodec,
    storage::codec::UnitCodec,
    types::NodeId,
};

type Codec = TcpEnvelopeCodec<(), (), UnitCodec, UnitCodec>;

fn codec() -> Codec {
    TcpEnvelopeCodec::new(UnitCodec, UnitCodec)
}

#[test]
/// Catches a wire-format mismatch that would silently drop ReadIndex quorum
/// confirmations when the core-generated request nonce is not preserved.
fn read_index_request_and_response_round_trip_through_tcp_codec() {
    let codec = codec();
    let request = Envelope {
        from: NodeId::must(1),
        to: NodeId::must(2),
        msg: Message::ReadIndex(ReadIndexRequest {
            term: 7,
            leader_id: NodeId::must(1),
            request_id: 42,
            context: b"opaque-read-context".to_vec(),
        }),
    };
    let response = Envelope {
        from: NodeId::must(2),
        to: NodeId::must(1),
        msg: Message::ReadIndexResponse(ReadIndexResponse {
            term: 7,
            request_id: 42,
            context: b"opaque-read-context".to_vec(),
        }),
    };

    assert_eq!(
        codec
            .decode_envelope(&codec.encode_envelope(&request).unwrap())
            .unwrap(),
        request
    );
    assert_eq!(
        codec
            .decode_envelope(&codec.encode_envelope(&response).unwrap())
            .unwrap(),
        response
    );
}

#[test]
/// Catches truncated ReadIndex frames being accepted with a default nonce or
/// partial context, which could acknowledge the wrong pending read.
fn truncated_read_index_frame_is_rejected() {
    let codec = codec();
    let envelope = Envelope {
        from: NodeId::must(1),
        to: NodeId::must(2),
        msg: Message::ReadIndex(ReadIndexRequest {
            term: 7,
            leader_id: NodeId::must(1),
            request_id: 42,
            context: b"opaque-read-context".to_vec(),
        }),
    };
    let encoded = codec.encode_envelope(&envelope).unwrap();

    assert!(
        codec
            .decode_envelope(&encoded[..encoded.len() - 1])
            .is_err()
    );
}

#[test]
/// Catches a transfer wire-format mismatch that could deliver the request to
/// the right voter while losing the term, transfer identity, or log frontier
/// used to reject stale and incomplete handoffs.
fn timeout_now_request_round_trips_through_tcp_codec() {
    let codec = codec();
    let request = Envelope {
        from: NodeId::must(1),
        to: NodeId::must(2),
        msg: Message::TimeoutNow(TimeoutNowRequest {
            term: 9,
            leader_id: NodeId::must(1),
            target_id: NodeId::must(2),
            transfer_id: 17,
            last_log_index: 42,
            last_log_term: 8,
        }),
    };

    assert_eq!(
        codec
            .decode_envelope(&codec.encode_envelope(&request).unwrap())
            .unwrap(),
        request
    );
}

#[test]
/// Catches the transport allocation bug where a peer can force the Raft wire
/// decoder to allocate an unbounded ReadIndex context before core validation.
fn oversized_read_index_context_is_rejected_before_decode() {
    let codec = codec();
    let envelope = Envelope {
        from: NodeId::must(1),
        to: NodeId::must(2),
        msg: Message::ReadIndex(ReadIndexRequest {
            term: 7,
            leader_id: NodeId::must(1),
            request_id: 42,
            context: vec![0; MAX_READ_INDEX_CONTEXT_BYTES + 1],
        }),
    };
    let encoded = codec.encode_envelope(&envelope).unwrap();

    assert!(codec.decode_envelope(&encoded).is_err());
}
