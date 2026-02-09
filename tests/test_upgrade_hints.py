"""
Tests for SDK upgrade notification features:
- Option 2: X-Tepilora-Min-SDK-Version header check
- Option 3: Upgrade hint on unknown action/namespace errors
"""

import json
import warnings

import httpx
import pytest

from Tepilora import TepiloraClient, AsyncTepiloraClient
from Tepilora.client import (
    _check_sdk_version,
    _parse_semver,
    _raise_for_error_response,
    _UPGRADE_HINT,
)
from Tepilora.errors import TepiloraAPIError
from Tepilora.version import __version__
import Tepilora.client as client_module


# ---------------------------------------------------------------------------
# Option 2: Server header version check
# ---------------------------------------------------------------------------

class TestParseSemver:
    def test_simple(self):
        assert _parse_semver("1.2.3") == (1, 2, 3)

    def test_zero(self):
        assert _parse_semver("0.0.0") == (0, 0, 0)

    def test_large(self):
        assert _parse_semver("10.20.300") == (10, 20, 300)

    def test_with_spaces(self):
        assert _parse_semver("  1.2.3  ") == (1, 2, 3)

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            _parse_semver("abc")


class TestCheckSdkVersion:
    """Test the _check_sdk_version function."""

    def setup_method(self):
        """Reset the global warning flag before each test."""
        client_module._upgrade_warned = False

    def test_no_header_no_warning(self):
        """No warning when header is absent."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _check_sdk_version({})
            assert len(w) == 0

    def test_current_version_no_warning(self):
        """No warning when SDK meets minimum version."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _check_sdk_version({"X-Tepilora-Min-SDK-Version": __version__})
            assert len(w) == 0

    def test_older_min_version_no_warning(self):
        """No warning when server requires older version."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _check_sdk_version({"X-Tepilora-Min-SDK-Version": "0.0.1"})
            assert len(w) == 0

    def test_newer_min_version_warns(self):
        """Warning when server requires newer version."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _check_sdk_version({"X-Tepilora-Min-SDK-Version": "99.99.99"})
            assert len(w) == 1
            assert "outdated" in str(w[0].message).lower()
            assert "pip install --upgrade tepilora" in str(w[0].message)

    def test_warns_only_once(self):
        """Warning is only emitted once per session."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _check_sdk_version({"X-Tepilora-Min-SDK-Version": "99.99.99"})
            _check_sdk_version({"X-Tepilora-Min-SDK-Version": "99.99.99"})
            _check_sdk_version({"X-Tepilora-Min-SDK-Version": "99.99.99"})
            assert len(w) == 1

    def test_invalid_header_value_no_crash(self):
        """Invalid version string doesn't crash."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _check_sdk_version({"X-Tepilora-Min-SDK-Version": "not-a-version"})
            assert len(w) == 0


class TestVersionCheckIntegration:
    """Test version check in actual client call flow."""

    def setup_method(self):
        client_module._upgrade_warned = False

    def test_call_checks_version_header(self):
        """Client.call() reads X-Tepilora-Min-SDK-Version from response."""
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={"success": True, "action": "test", "data": {}, "meta": {}},
                headers={"X-Tepilora-Min-SDK-Version": "99.0.0"},
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://test", transport=transport)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            client.call("securities.search", params={"query": "test"})
            assert len(w) == 1
            assert "99.0.0" in str(w[0].message)

    def test_no_warning_when_version_ok(self):
        """No warning when server accepts current SDK version."""
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={"success": True, "action": "test", "data": {}, "meta": {}},
                headers={"X-Tepilora-Min-SDK-Version": "0.0.1"},
            )

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://test", transport=transport)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            client.call("securities.search", params={"query": "test"})
            assert len(w) == 0


# ---------------------------------------------------------------------------
# Option 3: Upgrade hint on unknown action / namespace
# ---------------------------------------------------------------------------

class TestUnknownActionHint:
    """Test upgrade hints in error responses."""

    def test_404_unknown_action_has_hint(self):
        """404 with 'unknown action' message includes upgrade hint."""
        response = httpx.Response(
            404,
            json={"message": "Unknown action: foo.bar"},
            headers={"content-type": "application/json"},
        )
        with pytest.raises(TepiloraAPIError) as exc_info:
            _raise_for_error_response(response)
        assert "pip install --upgrade tepilora" in str(exc_info.value)

    def test_400_action_not_found_has_hint(self):
        """400 with 'action not found' message includes upgrade hint."""
        response = httpx.Response(
            400,
            json={"message": "Action not found: analytics.new_function"},
            headers={"content-type": "application/json"},
        )
        with pytest.raises(TepiloraAPIError) as exc_info:
            _raise_for_error_response(response)
        assert "pip install --upgrade tepilora" in str(exc_info.value)

    def test_400_invalid_action_has_hint(self):
        """400 with 'invalid action' message includes upgrade hint."""
        response = httpx.Response(
            400,
            json={"message": "Invalid action specified"},
            headers={"content-type": "application/json"},
        )
        with pytest.raises(TepiloraAPIError) as exc_info:
            _raise_for_error_response(response)
        assert "pip install --upgrade tepilora" in str(exc_info.value)

    def test_400_other_error_no_hint(self):
        """400 with unrelated error does NOT include upgrade hint."""
        response = httpx.Response(
            400,
            json={"message": "Missing required parameter: identifiers"},
            headers={"content-type": "application/json"},
        )
        with pytest.raises(TepiloraAPIError) as exc_info:
            _raise_for_error_response(response)
        assert "pip install --upgrade tepilora" not in str(exc_info.value)

    def test_500_error_no_hint(self):
        """500 error does NOT include upgrade hint."""
        response = httpx.Response(
            500,
            json={"message": "Internal server error"},
            headers={"content-type": "application/json"},
        )
        with pytest.raises(TepiloraAPIError) as exc_info:
            _raise_for_error_response(response)
        assert "pip install --upgrade tepilora" not in str(exc_info.value)

    def test_200_no_raise(self):
        """200 response does not raise."""
        response = httpx.Response(200)
        _raise_for_error_response(response)  # Should not raise


class TestUnknownNamespaceHint:
    """Test upgrade hints for unknown namespace access on client."""

    def test_sync_client_unknown_namespace(self):
        """Accessing unknown namespace suggests upgrade."""
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"success": True, "data": {}, "meta": {}})

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://test", transport=transport)

        with pytest.raises(AttributeError) as exc_info:
            _ = client.crypto  # Non-existent namespace
        assert "pip install --upgrade tepilora" in str(exc_info.value)
        assert "crypto" in str(exc_info.value)

    def test_async_client_unknown_namespace(self):
        """Async client also suggests upgrade for unknown namespace."""
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"success": True, "data": {}, "meta": {}})

        transport = httpx.MockTransport(handler)
        client = AsyncTepiloraClient(api_key="k", base_url="http://test", transport=transport)

        with pytest.raises(AttributeError) as exc_info:
            _ = client.crypto
        assert "pip install --upgrade tepilora" in str(exc_info.value)

    def test_known_namespaces_still_work(self):
        """Known namespaces are not affected by __getattr__."""
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"success": True, "data": {}, "meta": {}})

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://test", transport=transport)

        # These should NOT raise
        assert client.securities is not None
        assert client.analytics is not None
        assert client.portfolio is not None
        assert client.news is not None

    def test_private_attr_no_upgrade_hint(self):
        """Private attributes don't get upgrade hint."""
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"success": True, "data": {}, "meta": {}})

        transport = httpx.MockTransport(handler)
        client = TepiloraClient(api_key="k", base_url="http://test", transport=transport)

        with pytest.raises(AttributeError) as exc_info:
            _ = client._nonexistent
        assert "pip install --upgrade tepilora" not in str(exc_info.value)
