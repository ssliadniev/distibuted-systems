from enum import Enum


class NodeStatus(Enum):
    HEALTHY = "Healthy"
    SUSPECTED = "Suspected"
    UNHEALTHY = "Unhealthy"
