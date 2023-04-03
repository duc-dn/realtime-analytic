TABLE_MAPPINGS = {
    "cdc.myshop.orders": {
        "pk": "id"
    },
    "cdc.myshop.order_detail": {
        "pk1": "order_id",
        "pk2": "product_id"
    },
    "cdc.myshop.products": {
        "pk": "id"
    },
    "cdc.myshop.users": {
        "pk": "id"
    },
    "cdc.myshop.categories": {
        "pk": "id"
    },
    "cdc.myshop.brands": {
        "pk": "id"
    }
}

UX_DATA_TOPICS = [
    "cdc.myshop.orders", 
    "cdc.myshop.order_detail", 
    "cdc.myshop.products",
    "cdc.myshop.users",
    "cdc.myshop.brands",
    "cdc.myshop.categories"
]