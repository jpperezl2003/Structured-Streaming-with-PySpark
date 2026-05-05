"""
Shared schema definition for the Food Delivery streaming project.
This schema is used by the PySpark consumer to parse JSON events received from Kafka.
"""

delivery_event_columns = [
    ("order_id", "int"),
    ("customer_id", "int"),
    ("restaurant_id", "int"),
    ("delivery_driver_id", "int"),
    ("event_type", "string"),
    ("order_amount", "double"),
    ("delivery_zone", "string"),
    ("payment_method", "string"),
    ("timestamp", "string")
]