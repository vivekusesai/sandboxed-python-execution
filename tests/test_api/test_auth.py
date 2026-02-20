"""Tests for authentication endpoints."""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
class TestAuthEndpoints:
    """Test authentication endpoints."""

    async def test_register_success(self, client: AsyncClient):
        """User registration should succeed."""
        response = await client.post(
            "/api/v1/auth/register",
            json={
                "email": "newuser@example.com",
                "password": "securepassword123",
            },
        )

        assert response.status_code == 201
        data = response.json()
        assert data["email"] == "newuser@example.com"
        assert "id" in data
        assert "password_hash" not in data

    async def test_register_duplicate_email(self, client: AsyncClient, test_user):
        """Duplicate email should fail."""
        response = await client.post(
            "/api/v1/auth/register",
            json={
                "email": "test@example.com",
                "password": "anotherpassword",
            },
        )

        assert response.status_code == 400
        assert "already registered" in response.json()["detail"]

    async def test_register_invalid_email(self, client: AsyncClient):
        """Invalid email should fail."""
        response = await client.post(
            "/api/v1/auth/register",
            json={
                "email": "not-an-email",
                "password": "securepassword123",
            },
        )

        assert response.status_code == 422

    async def test_register_short_password(self, client: AsyncClient):
        """Short password should fail."""
        response = await client.post(
            "/api/v1/auth/register",
            json={
                "email": "user@example.com",
                "password": "short",
            },
        )

        assert response.status_code == 422

    async def test_login_success(self, client: AsyncClient, test_user):
        """Valid login should return token."""
        response = await client.post(
            "/api/v1/auth/login",
            data={
                "username": "test@example.com",
                "password": "testpassword",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert data["token_type"] == "bearer"

    async def test_login_invalid_password(self, client: AsyncClient, test_user):
        """Invalid password should fail."""
        response = await client.post(
            "/api/v1/auth/login",
            data={
                "username": "test@example.com",
                "password": "wrongpassword",
            },
        )

        assert response.status_code == 401

    async def test_login_nonexistent_user(self, client: AsyncClient):
        """Nonexistent user should fail."""
        response = await client.post(
            "/api/v1/auth/login",
            data={
                "username": "nobody@example.com",
                "password": "anypassword",
            },
        )

        assert response.status_code == 401

    async def test_protected_endpoint_without_token(self, client: AsyncClient):
        """Protected endpoint without token should fail."""
        response = await client.get("/api/v1/tables")

        assert response.status_code == 401

    async def test_protected_endpoint_with_token(self, client: AsyncClient, auth_headers):
        """Protected endpoint with valid token should succeed."""
        response = await client.get(
            "/api/v1/tables",
            headers=auth_headers,
        )

        assert response.status_code == 200
