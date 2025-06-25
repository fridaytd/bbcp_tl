from pydantic import BaseModel


from typing import List, Optional
from datetime import datetime


class Price(BaseModel):
    min: float
    max: float
    currencyCode: str


class Product(BaseModel):
    id: int
    name: str
    minFaceValue: float
    maxFaceValue: float
    count: Optional[int]
    price: Price
    modifiedDate: datetime


class Category(BaseModel):
    id: int
    name: str
    description: Optional[str]


class Brand(BaseModel):
    internalId: str
    name: str
    countryCode: str
    currencyCode: str
    description: Optional[str]
    disclaimer: Optional[str]
    redemptionInstructions: Optional[str]
    terms: Optional[str]
    logoUrl: Optional[str]
    modifiedDate: str
    products: List[Product]
    categories: List[Category]


class CatalogResponse(BaseModel):
    brands: List[Brand]


class FriProduct(BaseModel):
    id: int
    name: str
    countryCode: str
    price: Price
