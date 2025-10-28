from rest_framework.pagination import LimitOffsetPagination


class DefaultLimitOffsetPagination(LimitOffsetPagination):
    """
    Custom LimitOffsetPagination optimized for large datasets.
    """

    default_limit = 20
    max_limit = 100
    limit_query_param = "limit"
    offset_query_param = "offset"
