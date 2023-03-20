TABLE_MAPPINGS = {
    "cdc.inventory.orders": {
        "pk": "order_number"
    },
    "cdc.inventory.products": {
        "pk": "id"
    },
    "cdc.inventory.customers": {
        "pk": "id"
    }
}

UX_DATA_TOPICS = [
    "cdc.inventory.orders", 
    "cdc.inventory.products",
    "cdc.inventory.customers"
]