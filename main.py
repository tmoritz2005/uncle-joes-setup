from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from pydantic import BaseModel
from typing import Optional
from datetime import date
import bcrypt

PROJECT_ID = "mgmt545-groupproject"
DATASET = "unlce_joes"


app = FastAPI(title="Uncle Joe's Coffee API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


client = bigquery.Client(project="mgmt545-groupproject")

def run_query(query: str, params: list = []):
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    result = client.query(query, job_config=job_config).result()
    return [dict(row) for row in result]

#Pydantics

class LoginRequest(BaseModel):
    email: str
    password: str

class LoginResponse(BaseModel):
    member_id: str
    name: str
    email: str
    home_store: int

class MenuItem(BaseModel):
    id: str
    name: str
    category: str
    size: Optional[str] = None
    calories: Optional[int] = None
    price: float

class Location(BaseModel):
    id: str
    city: str
    state: str
    monday_open: Optional[int] = None
    monday_close: Optional[int] = None
    tuesday_open: Optional[int] = None
    tuesday_close: Optional[int] = None   
    wednesday_open: Optional[int] = None
    wednesday_close: Optional[int] = None
    thursday_open: Optional[int] = None
    thursday_close: Optional[int] = None
    friday_open: Optional[int] = None
    friday_close: Optional[int] = None
    saturday_open: Optional[int] = None
    saturday_close: Optional[int] = None
    sunday_open: Optional[int] = None
    sunday_close: Optional[int] = None
    drive_thru: bool
    door_dash: bool

class Order(BaseModel):
    hours: Optional[str] = None
    amenities: Optional[str] = None

class OrderItem(BaseModel):
    menu_item_id: str
    item_name: str
    quantity: int
    price: float

class OrderHistory(BaseModel):
    order_id: str
    store_id: int
    order_date: date
    order_total: float
    items: list[OrderItem] = []

class PointsBalance(BaseModel):
    member_id: str
    total_points: int


@app.get("/")
def root():
    return {"message": "Uncle Joe's Coffee API is running"}

#User Authorization Endpoint - Note that project name might need to be changed

@app.post("/login", response_model=LoginResponse)
def login(request: LoginRequest):
    query = """
        SELECT id, name, email, home_store, password_hash
        FROM mgmt545-groupproject.unlce_joes.members     
        WHERE email = @email
    """
    params = [bigquery.ScalarQueryParameter("email", "STRING", request.email)]
    results = run_query(query, params)

    if not results:
        raise HTTPException(status_code=401, detail="Invalid email or password")

    member = results[0]
    password_match = bcrypt.checkpw(
        request.password.encode(),
        member["password_hash"].encode()
    )

    if not password_match:
        raise HTTPException(status_code=401, detail="Invalid email or password")

    return LoginResponse(
        member_id=member["id"],
        name=member["name"],
        email=member["email"],
        home_store=member["home_store"]
    )

#Menu Endpoint 

@app.get("/menu", response_model=list[MenuItem])
def get_menu():
    query = """
        SELECT id, name, category, size, calories, price
        FROM mgmt545-groupproject.unlce_joes.menu
    """
    return run_query(query)

#Locations Endpoint 

#next week lets simplify this for users by combining hours and drive_thru adn door_dash into
#weekly_hours and amenities respectively
@app.get("/locations", response_model=list[Location])
def get_locations():
    query = """
        SELECT 
        id,
        city, 
        state, 
        location_map_address as address, 
        hours_monday_open as monday_open, 
        hours_monday_close as monday_close,
        hours_tuesday_open as tuesday_open, 
        hours_tuesday_close as tuesday_close,
        hours_wednesday_open as wednesday_open, 
        hours_wednesday_close as wednesday_close,
        hours_thursday_open as thursday_open, 
        hours_thursday_close as thursday_close,
        hours_friday_open as friday_open, 
        hours_friday_close as friday_close,
        hours_saturday_open as saturday_open, 
        hours_saturday_close as saturday_close,
        hours_sunday_open as sunday_open, 
        hours_sunday_close as sunday_close,
        drive_thru,
        door_dash
        FROM `mgmt545-groupproject.unlce_joes.locations`
    """
    return run_query(query)

#Order History Endpoint

@app.get("/members/{member_id}/orders", response_model=list[OrderHistory])
def get_orders(member_id: int):
    query = """
        SELECT
            o.order_id,
            o.store_id,
            o.order_date,
            o.order_total,
            oi.menu_item_id,
            oi.item_name,
            oi.quantity,
            oi.price
        FROM mgmt545-groupproject.unlce_joes.orders o
        JOIN mgmt545-groupproject.unlce_joes.order_items oi
            ON o.order_id = oi.order_id
        WHERE o.member_id = @member_id
        ORDER BY o.order_date DESC
    """
    params = [bigquery.ScalarQueryParameter("member_id", "INT64", member_id)]
    rows = run_query(query, params)

    orders = {}
    for row in rows:
        oid = row["order_id"]
        if oid not in orders:
            orders[oid] = Order(
                order_id=oid,
                store_id=row["store_id"],
                order_date=row["order_date"],
                order_total=row["order_total"],
                items=[]
            )
        orders[oid].items.append(OrderItem(
            menu_item_id=row["menu_item_id"],
            item_name=row["item_name"],
            quantity=row["quantity"],
            price=row["price"]
        ))

    return list(orders.values())

#Points Balance Endpoint

@app.get("/members/{member_id}/points", response_model=PointsBalance)
def get_points(member_id: int):
    query = """
        SELECT SUM(FLOOR(order_total)) as total_points
        FROM mgmt545-groupproject.unlce_joes.orders
        WHERE member_id = @member_id
    """
    params = [bigquery.ScalarQueryParameter("member_id", "INT64", member_id)]
    results = run_query(query, params)
    total = results[0]["total_points"] or 0
    return PointsBalance(member_id=member_id, total_points=int(total))
