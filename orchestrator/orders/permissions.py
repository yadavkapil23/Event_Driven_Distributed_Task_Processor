from django.conf import settings
from rest_framework.permissions import BasePermission


class HasAPIKey(BasePermission):
    message = 'Missing or invalid API key.'

    def has_permission(self, request, view):
        return request.headers.get('X-API-Key') == settings.API_KEY
