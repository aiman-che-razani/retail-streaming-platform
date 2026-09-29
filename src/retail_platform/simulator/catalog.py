"""Deterministic product catalog generation.

Products get a Zipf-like popularity so the data has genuine top sellers and a long tail of
underperformers — the analytics in Phase 7 depend on that shape.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal

from retail_platform.contracts.models import UnitOfMeasure

CENT = Decimal("0.01")


def money(value: float | Decimal) -> Decimal:
    return Decimal(str(value)).quantize(CENT, rounding=ROUND_HALF_UP)


@dataclass(frozen=True, slots=True)
class SubcategorySpec:
    category: str
    subcategory: str
    min_price: float
    max_price: float
    uom: UnitOfMeasure
    descriptors: tuple[str, ...]
    sizes: tuple[str, ...]


# (category, subcategory, price range MYR, unit of measure, descriptors, pack sizes)
SUBCATEGORIES: tuple[SubcategorySpec, ...] = (
    SubcategorySpec("Beverages", "Instant Drinks", 8.5, 24.9, UnitOfMeasure.PACK,
                    ("Teh Tarik 3-in-1", "White Coffee 3-in-1", "Chocolate Malt", "Kopi O"),
                    ("10 x 30g", "15 x 40g", "20 x 25g")),
    SubcategorySpec("Beverages", "Soft Drinks", 1.8, 9.9, UnitOfMeasure.EACH,
                    ("Cola", "Sarsi", "Orange Soda", "Lemon Lime", "Isotonic"),
                    ("325ml", "500ml", "1.5L")),
    SubcategorySpec("Beverages", "Mineral Water", 0.9, 6.5, UnitOfMeasure.EACH,
                    ("Mineral Water", "Sparkling Water", "Alkaline Water"),
                    ("500ml", "1.5L", "5L")),
    SubcategorySpec("Beverages", "Juices", 3.5, 12.9, UnitOfMeasure.EACH,
                    ("Orange Juice", "Guava Juice", "Apple Juice", "Mango Nectar"),
                    ("250ml", "1L")),
    SubcategorySpec("Snacks", "Chips", 2.5, 9.9, UnitOfMeasure.EACH,
                    ("Potato Chips Original", "Cassava Crisps", "Prawn Crackers", "Corn Puffs"),
                    ("60g", "120g", "160g")),
    SubcategorySpec("Snacks", "Biscuits", 3.2, 15.9, UnitOfMeasure.PACK,
                    ("Cream Crackers", "Chocolate Wafers", "Butter Cookies", "Marie Biscuits"),
                    ("200g", "400g", "700g")),
    SubcategorySpec("Snacks", "Confectionery", 1.5, 12.5, UnitOfMeasure.EACH,
                    ("Milk Chocolate Bar", "Fruit Gummies", "Mint Candy", "Durian Toffee"),
                    ("35g", "100g", "250g")),
    SubcategorySpec("Groceries", "Rice & Grains", 12.9, 45.0, UnitOfMeasure.PACK,
                    ("Fragrant White Rice", "Basmati Rice", "Brown Rice", "Glutinous Rice"),
                    ("2kg", "5kg", "10kg")),
    SubcategorySpec("Groceries", "Noodles", 3.9, 12.9, UnitOfMeasure.PACK,
                    ("Curry Instant Noodles", "Mee Goreng", "Tom Yam Noodles", "Rice Vermicelli"),
                    ("5 x 80g", "400g")),
    SubcategorySpec("Groceries", "Cooking Oil", 7.9, 32.0, UnitOfMeasure.EACH,
                    ("Palm Cooking Oil", "Canola Oil", "Sunflower Oil"),
                    ("1kg", "2kg", "5kg")),
    SubcategorySpec("Groceries", "Condiments & Sauces", 2.2, 14.9, UnitOfMeasure.EACH,
                    ("Sambal Paste", "Soy Sauce", "Oyster Sauce", "Chilli Sauce", "Kicap Manis"),
                    ("250ml", "340g", "500ml")),
    SubcategorySpec("Groceries", "Canned Food", 3.5, 11.9, UnitOfMeasure.EACH,
                    ("Sardines in Tomato", "Baked Beans", "Chicken Curry", "Tuna Chunks"),
                    ("155g", "425g")),
    SubcategorySpec("Dairy & Frozen", "Milk", 3.9, 16.9, UnitOfMeasure.EACH,
                    ("Fresh Milk", "Low Fat Milk", "Chocolate Milk", "Soy Milk"),
                    ("1L", "2L")),
    SubcategorySpec("Dairy & Frozen", "Ice Cream", 6.9, 24.9, UnitOfMeasure.EACH,
                    ("Vanilla Ice Cream", "Durian Ice Cream", "Chocolate Cone", "Mango Sorbet"),
                    ("4 x 70ml", "1.5L")),
    SubcategorySpec("Dairy & Frozen", "Frozen Food", 7.5, 29.9, UnitOfMeasure.PACK,
                    ("Chicken Nuggets", "Frozen Paratha", "Fish Balls", "Curry Puffs"),
                    ("500g", "1kg")),
    SubcategorySpec("Personal Care", "Hair Care", 9.9, 39.9, UnitOfMeasure.EACH,
                    ("Anti-Dandruff Shampoo", "Herbal Shampoo", "Conditioner"),
                    ("300ml", "680ml")),
    SubcategorySpec("Personal Care", "Oral Care", 3.9, 19.9, UnitOfMeasure.EACH,
                    ("Toothpaste Herbal", "Toothpaste Whitening", "Mouthwash", "Toothbrush Soft"),
                    ("100g", "225g", "500ml")),
    SubcategorySpec("Personal Care", "Bath", 4.5, 26.9, UnitOfMeasure.EACH,
                    ("Shower Foam", "Antibacterial Soap", "Body Wash Lavender"),
                    ("3 x 80g", "600ml", "1L")),
    SubcategorySpec("Household", "Detergent", 9.9, 49.9, UnitOfMeasure.EACH,
                    ("Liquid Detergent", "Powder Detergent", "Fabric Softener"),
                    ("1.8kg", "3.6kg", "2L")),
    SubcategorySpec("Household", "Tissue & Paper", 4.9, 29.9, UnitOfMeasure.PACK,
                    ("Facial Tissue", "Toilet Roll", "Kitchen Towel"),
                    ("4 x 100s", "10 rolls", "2 rolls")),
    SubcategorySpec("Household", "Cleaning", 3.9, 21.9, UnitOfMeasure.EACH,
                    ("Floor Cleaner", "Dishwashing Liquid", "Glass Cleaner"),
                    ("500ml", "900ml", "2L")),
    SubcategorySpec("Baby Care", "Diapers", 24.9, 79.9, UnitOfMeasure.PACK,
                    ("Tape Diapers M", "Pants Diapers L", "Pants Diapers XL"),
                    ("32s", "54s", "64s")),
    SubcategorySpec("Baby Care", "Baby Food", 6.9, 49.9, UnitOfMeasure.EACH,
                    ("Infant Formula Stage 1", "Rice Cereal", "Fruit Puree"),
                    ("120g", "400g", "900g")),
)  # fmt: skip

BRANDS: tuple[str, ...] = (
    "Warisan Emas", "Kampung Pilihan", "Sinar Harian", "Tropika", "Bunga Raya", "Lembah Hijau",
    "Pelangi", "Mutiara", "Seri Murni", "Anggun", "Nusantara", "Suria", "Cahaya", "KedaiKita",
)  # fmt: skip


@dataclass(slots=True)
class ProductState:
    """Mutable simulator-side product master record (owned by the simulator instance)."""

    product_id: str
    product_name: str
    brand: str
    category: str
    subcategory: str
    list_price: Decimal
    unit_cost: Decimal
    unit_of_measure: UnitOfMeasure
    is_active: bool
    popularity: float  # relative sales weight (Zipf-like)
    reorder_point: int
    reorder_quantity: int


def build_catalog(rng: random.Random, count: int) -> list[ProductState]:
    """Generate `count` products with SKU-10000.. ids. Deterministic for a given RNG seed."""
    products: list[ProductState] = []
    ranks = list(range(1, count + 1))
    rng.shuffle(ranks)  # popularity rank is independent of category
    for index in range(count):
        spec = SUBCATEGORIES[index % len(SUBCATEGORIES)]
        brand = rng.choice(BRANDS)
        descriptor = rng.choice(spec.descriptors)
        size = rng.choice(spec.sizes)
        price = money(rng.uniform(spec.min_price, spec.max_price))
        margin = rng.uniform(0.18, 0.42)
        popularity = 1.0 / (ranks[index] ** 0.85)
        # Faster sellers carry more stock.
        reorder_point = max(6, int(40 * popularity**0.5) + rng.randint(0, 6))
        products.append(
            ProductState(
                product_id=f"SKU-{10000 + index:05d}",
                product_name=f"{brand} {descriptor} ({size})",
                brand=brand,
                category=spec.category,
                subcategory=spec.subcategory,
                list_price=price,
                unit_cost=money(price * Decimal(str(round(1 - margin, 4)))),
                unit_of_measure=spec.uom,
                is_active=True,
                popularity=popularity,
                reorder_point=reorder_point,
                reorder_quantity=reorder_point * 3,
            )
        )
    return products
