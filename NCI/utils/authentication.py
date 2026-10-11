from django.conf import settings
from django.http import JsonResponse
from functools import wraps


def require_api_token(view_func):
    @wraps(view_func)
    def wrapper(request, *args, **kwargs):
        auth_header = request.headers.get("Authorization")

        if not auth_header:
            return JsonResponse(
                {"detail": "Authorization token is required"},
                status=401
            )

        if not auth_header.startswith("Bearer "):
            return JsonResponse(
                {"detail": "Invalid authorization format"},
                status=401
            )

        token = auth_header.split(" ", 1)[1]

        if token != settings.API_SECRET_TOKEN:
            return JsonResponse(
                {"detail": "Invalid token"},
                status=401
            )

        return view_func(request, *args, **kwargs)

    return wrapper