"""Focused regression contracts for auth response typing and redaction."""

from __future__ import annotations

import json
import unittest
from typing import Any

from mtf_lab.data.ctrader_config import CTraderConfig
from mtf_lab.data.ctrader_errors import AuthState, CTraderAuthError
from mtf_lab.data.ctrader_protocol import PAYLOAD, WireMessage
from mtf_lab.data.ctrader_session import CTraderClient
from mtf_lab.data.ctrader_transport import DeterministicTransport
from mtf_lab.data.protobuf_generated import OpenApiMessages_pb2 as proto


def _account_discovery_response(*, mapping: bool) -> WireMessage:
    if mapping:
        payload: Any = {
            "accessToken": "SYNTHETIC_ACCESS",
            "permissionScope": "SCOPE_TRADE",
            "ctidTraderAccount": [{"ctidTraderAccountId": 7, "isLive": False}],
        }
    else:
        payload = proto.ProtoOAGetAccountListByAccessTokenRes(
            accessToken="SYNTHETIC_ACCESS",
            permissionScope="SCOPE_TRADE",
        )
        payload.ctidTraderAccount.add(ctidTraderAccountId=7, isLive=False)
    return WireMessage(PAYLOAD["PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES"], payload, "request")


def _client(handler: Any) -> CTraderClient:
    return CTraderClient(
        CTraderConfig(
            client_id="synthetic-client",
            client_secret_ref="synthetic-secret-ref",
            access_token_ref="synthetic-token-ref",
            account_id=7,
            symbol_id=99,
        ),
        transport=DeterministicTransport(handler),
    )


class AuthResponseProtocolTests(unittest.TestCase):
    def test_official_mapping_and_protobuf_auth_responses_still_authenticate(self) -> None:
        for mapping in (True, False):
            with self.subTest(mapping=mapping):

                def handler(message: WireMessage, *, mapping: bool = mapping) -> WireMessage:
                    if message.payload_type_id == PAYLOAD["PROTO_OA_APPLICATION_AUTH_REQ"]:
                        payload: Any = {} if mapping else proto.ProtoOAApplicationAuthRes()
                        return WireMessage(PAYLOAD["PROTO_OA_APPLICATION_AUTH_RES"], payload, message.client_msg_id)
                    if message.payload_type_id == PAYLOAD["PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ"]:
                        response = _account_discovery_response(mapping=mapping)
                        return WireMessage(response.payload_type, response.payload, message.client_msg_id)
                    if message.payload_type_id == PAYLOAD["PROTO_OA_ACCOUNT_AUTH_REQ"]:
                        payload = (
                            {"ctidTraderAccountId": 7}
                            if mapping
                            else proto.ProtoOAAccountAuthRes(ctidTraderAccountId=7)
                        )
                        return WireMessage(PAYLOAD["PROTO_OA_ACCOUNT_AUTH_RES"], payload, message.client_msg_id)
                    raise AssertionError(message.payload_type_name)

                client = _client(handler)
                client.connect()
                try:
                    self.assertEqual(
                        client.authenticate(
                            secret_provider=lambda _ref: "SYNTHETIC_SECRET",
                            token_provider=lambda _ref: "SYNTHETIC_ACCESS",
                        ),
                        AuthState.AUTHENTICATED,
                    )
                    self.assertEqual(client.authenticated_session_evidence().account_id, 7)
                finally:
                    client.close()

    def test_wrong_application_response_type_fails_before_application_auth(self) -> None:
        def handler(message: WireMessage) -> WireMessage:
            return WireMessage(PAYLOAD["PROTO_OA_ACCOUNT_AUTH_RES"], {"ctidTraderAccountId": 7}, message.client_msg_id)

        transport = DeterministicTransport(handler)
        client = CTraderClient(
            CTraderConfig(
                client_id="synthetic-client",
                client_secret_ref="synthetic-secret-ref",
                access_token_ref="synthetic-token-ref",
                account_id=7,
                symbol_id=99,
            ),
            transport=transport,
        )
        client.connect()
        try:
            with self.assertRaises(CTraderAuthError):
                client.authenticate(
                    secret_provider=lambda _ref: "SYNTHETIC_SECRET",
                    token_provider=lambda _ref: "SYNTHETIC_ACCESS",
                )
            self.assertEqual(client.status.auth, AuthState.INVALID)
            self.assertFalse(client._application_authenticated)
            self.assertEqual(len(transport.sent), 1)
        finally:
            client.close()

    def test_wrong_discovery_response_type_is_not_normalized_or_cached(self) -> None:
        def handler(message: WireMessage) -> WireMessage:
            if message.payload_type_id == PAYLOAD["PROTO_OA_APPLICATION_AUTH_REQ"]:
                return WireMessage(PAYLOAD["PROTO_OA_APPLICATION_AUTH_RES"], {}, message.client_msg_id)
            return WireMessage(
                PAYLOAD["PROTO_OA_SYMBOLS_LIST_RES"],
                {"ctidTraderAccountId": 7},
                message.client_msg_id,
            )

        client = _client(handler)
        client.connect()
        try:
            with self.assertRaises(CTraderAuthError):
                client.authenticate(
                    secret_provider=lambda _ref: "SYNTHETIC_SECRET",
                    token_provider=lambda _ref: "SYNTHETIC_ACCESS",
                    authorize_selected=False,
                )
            self.assertEqual(client.status.auth, AuthState.INVALID)
            self.assertIsNone(client._account_discovery)
        finally:
            client.close()

    def test_wrong_account_response_type_does_not_mint_session_proof(self) -> None:
        def handler(message: WireMessage) -> WireMessage:
            if message.payload_type_id == PAYLOAD["PROTO_OA_APPLICATION_AUTH_REQ"]:
                return WireMessage(
                    PAYLOAD["PROTO_OA_APPLICATION_AUTH_RES"], proto.ProtoOAApplicationAuthRes(), message.client_msg_id
                )
            if message.payload_type_id == PAYLOAD["PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ"]:
                response = _account_discovery_response(mapping=False)
                return WireMessage(response.payload_type, response.payload, message.client_msg_id)
            wrong = proto.ProtoOASymbolsListRes(ctidTraderAccountId=7)
            return WireMessage(PAYLOAD["PROTO_OA_ACCOUNT_AUTH_RES"], wrong, message.client_msg_id)

        client = _client(handler)
        client.connect()
        try:
            with self.assertRaises(CTraderAuthError):
                client.authenticate(
                    secret_provider=lambda _ref: "SYNTHETIC_SECRET",
                    token_provider=lambda _ref: "SYNTHETIC_ACCESS",
                )
            self.assertEqual(client.status.auth, AuthState.INVALID)
            self.assertIsNone(client._session_evidence)
            self.assertIsNone(client._authenticated_account_id)
            with self.assertRaises(CTraderAuthError):
                client.authenticated_session_evidence()
        finally:
            client.close()

    def test_public_authorize_account_rejects_wrong_type_and_invalidates_existing_proof(self) -> None:
        wrong = False

        def handler(message: WireMessage) -> WireMessage:
            if message.payload_type_id == PAYLOAD["PROTO_OA_APPLICATION_AUTH_REQ"]:
                return WireMessage(
                    PAYLOAD["PROTO_OA_APPLICATION_AUTH_RES"], proto.ProtoOAApplicationAuthRes(), message.client_msg_id
                )
            if message.payload_type_id == PAYLOAD["PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ"]:
                response = _account_discovery_response(mapping=False)
                return WireMessage(response.payload_type, response.payload, message.client_msg_id)
            if wrong:
                payload = proto.ProtoOASymbolsListRes(ctidTraderAccountId=7)
                return WireMessage(PAYLOAD["PROTO_OA_ACCOUNT_AUTH_RES"], payload, message.client_msg_id)
            return WireMessage(
                PAYLOAD["PROTO_OA_ACCOUNT_AUTH_RES"],
                proto.ProtoOAAccountAuthRes(ctidTraderAccountId=7),
                message.client_msg_id,
            )

        client = _client(handler)
        client.connect()
        try:
            self.assertEqual(
                client.authenticate(
                    secret_provider=lambda _ref: "SYNTHETIC_SECRET",
                    token_provider=lambda _ref: "SYNTHETIC_ACCESS",
                    authorize_selected=False,
                ),
                AuthState.ACCOUNT_REQUIRED,
            )
            self.assertEqual(
                client.authorize_account(
                    7,
                    token_provider=lambda _ref: "SYNTHETIC_ACCESS",
                ),
                AuthState.AUTHENTICATED,
            )
            self.assertEqual(client.authenticated_session_evidence().account_id, 7)

            wrong = True
            with self.assertRaises(CTraderAuthError):
                client.authorize_account(
                    7,
                    token_provider=lambda _ref: "SYNTHETIC_ACCESS",
                )
            self.assertEqual(client.status.auth, AuthState.INVALID)
            self.assertIsNone(client._session_evidence)
            self.assertIsNone(client._authenticated_account_id)
            with self.assertRaises(CTraderAuthError):
                client.authenticated_session_evidence()
        finally:
            client.close()


class ProtobufRedactionTests(unittest.TestCase):
    def test_generated_protobuf_secrets_are_redacted_recursively(self) -> None:
        application = WireMessage(
            PAYLOAD["PROTO_OA_APPLICATION_AUTH_REQ"],
            proto.ProtoOAApplicationAuthReq(clientId="synthetic-client", clientSecret="SYNTHETIC_SECRET"),
        )
        account = WireMessage(
            PAYLOAD["PROTO_OA_ACCOUNT_AUTH_REQ"],
            proto.ProtoOAAccountAuthReq(ctidTraderAccountId=7, accessToken="SYNTHETIC_ACCESS"),
        )
        for message in (application, account):
            rendered = message.to_dict(redact=True)
            text = json.dumps(rendered, sort_keys=True)
            self.assertIsInstance(rendered["payload"], dict)
            self.assertNotIn("SYNTHETIC_SECRET", text)
            self.assertNotIn("SYNTHETIC_ACCESS", text)
            for key in ("clientSecret", "accessToken"):
                if key in rendered["payload"]:
                    self.assertEqual(rendered["payload"][key], "<redacted>")

    def test_nested_mapping_and_list_protobuf_secrets_are_redacted(self) -> None:
        message = WireMessage(
            PAYLOAD["PROTO_OA_APPLICATION_AUTH_REQ"],
            {
                "nested": [
                    proto.ProtoOAApplicationAuthReq(clientSecret="SYNTHETIC_SECRET"),
                    {"inner": proto.ProtoOAAccountAuthReq(accessToken="SYNTHETIC_ACCESS")},
                ]
            },
        )
        rendered = message.to_dict(redact=True)
        text = json.dumps(rendered, sort_keys=True)
        self.assertNotIn("SYNTHETIC_SECRET", text)
        self.assertNotIn("SYNTHETIC_ACCESS", text)
        nested = rendered["payload"]["nested"]
        self.assertEqual(nested[0]["clientSecret"], "<redacted>")
        self.assertEqual(nested[1]["inner"]["accessToken"], "<redacted>")


if __name__ == "__main__":
    unittest.main()
