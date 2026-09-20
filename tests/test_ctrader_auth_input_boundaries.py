from __future__ import annotations

import unittest
from urllib.parse import urlsplit

from mtf_lab.data.ctrader_accounts import normalize_account_payload
from mtf_lab.data.ctrader_errors import CTraderDataError
from mtf_lab.ops.ctrader_oauth_http import OAuthHTTPError, build_token_request

try:
    from mtf_lab.data.protobuf_generated import OpenApiMessages_pb2 as _messages
except ImportError:  # pragma: no cover - optional codec runtime
    _messages = None


class CTraderAccountInputBoundaryTests(unittest.TestCase):
    def test_account_ids_accept_positive_int_and_decimal_string(self) -> None:
        self.assertEqual(
            normalize_account_payload(
                {
                    "ctidTraderAccount": [
                        {"ctidTraderAccountId": 7, "isLive": False},
                        {"account_id": " 8 ", "is_live": "demo"},
                    ]
                }
            )["records"],
            [
                {"account_id": 7, "environment": "DEMO"},
                {"account_id": 8, "environment": "DEMO"},
            ],
        )

    def test_account_ids_reject_bool_float_and_non_decimal_string(self) -> None:
        for value in (True, 1.9, "1.9", "+7", "1_000", "٧", "", 2**63, "9223372036854775808"):
            with self.subTest(value=value), self.assertRaises(CTraderDataError):
                normalize_account_payload({"ctidTraderAccount": [{"account_id": value, "isLive": False}]})

    def test_account_id_aliases_must_agree(self) -> None:
        with self.assertRaisesRegex(CTraderDataError, "aliases"):
            normalize_account_payload(
                {
                    "ctidTraderAccount": [
                        {"ctidTraderAccountId": 7, "account_id": 8, "isLive": False},
                    ]
                }
            )

    @unittest.skipUnless(_messages is not None, "codec Protobuf generado no instalado")
    def test_generated_protobuf_repeated_field_and_false_is_live_remain_compatible(self) -> None:
        messages = _messages
        if messages is None:  # pragma: no cover - skip decorator handles this branch
            self.skipTest("codec Protobuf generado no instalado")
        payload = messages.ProtoOAGetAccountListByAccessTokenRes()
        payload.ctidTraderAccount.add(ctidTraderAccountId=7, isLive=False)

        self.assertEqual(
            normalize_account_payload(payload),
            {"records": [{"account_id": 7, "environment": "DEMO"}], "permissionScope": None},
        )

        missing_id = _messages.ProtoOAGetAccountListByAccessTokenRes()
        missing_id.ctidTraderAccount.add(isLive=False)
        with self.assertRaises(CTraderDataError):
            normalize_account_payload(missing_id)


class CTraderOAuthInputBoundaryTests(unittest.TestCase):
    _URL = "https://openapi.ctrader.com/apps/token"
    _PARAMS = {
        "grant_type": "refresh_token",
        "refresh_token": "refresh-fixture",
        "client_id": "client-fixture",
        "client_secret": "secret-fixture",
    }

    def test_valid_params_keep_documented_method_and_query_order(self) -> None:
        request = build_token_request(self._URL, self._PARAMS)

        self.assertEqual(request.method, "POST")
        self.assertEqual(
            urlsplit(request.full_url).query,
            "grant_type=refresh_token&refresh_token=refresh-fixture&client_id=client-fixture&client_secret=secret-fixture",
        )

    def test_opaque_code_and_secret_bytes_are_not_trimmed(self) -> None:
        params = {
            "grant_type": "authorization_code",
            "code": " code-fixture ",
            "redirect_uri": "http://127.0.0.1:8767/oauth/callback",
            "client_id": "client-fixture",
            "client_secret": " secret-fixture ",
        }

        request = build_token_request(self._URL, params)

        query = urlsplit(request.full_url).query
        self.assertIn("code=+code-fixture+", query)
        self.assertIn("client_secret=+secret-fixture+", query)

    def test_oauth_params_reject_non_strings_and_blank_values(self) -> None:
        for value in (None, False, 1.9, object(), " \t"):
            params = dict(self._PARAMS)
            params["client_secret"] = value  # type: ignore[assignment]
            with self.subTest(value=type(value).__name__), self.assertRaises(OAuthHTTPError):
                build_token_request(self._URL, params)

    def test_grant_type_must_be_string_and_match_the_allowed_shape(self) -> None:
        for value in (None, True, 1, "password", " "):
            params = dict(self._PARAMS)
            params["grant_type"] = value  # type: ignore[assignment]
            with self.subTest(value=value), self.assertRaises(OAuthHTTPError):
                build_token_request(self._URL, params)

        params = dict(self._PARAMS)
        params[1] = "unexpected"  # type: ignore[index]
        with self.assertRaises(OAuthHTTPError):
            build_token_request(self._URL, params)

        with self.assertRaises(OAuthHTTPError):
            build_token_request(self._URL, None)  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
