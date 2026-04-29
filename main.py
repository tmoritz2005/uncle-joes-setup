from fastapi import FastAPI, HTTPException
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from google.api_core.exceptions import NotFound,BadRequest,GoogleAPIError
from pydantic import BaseModel
from typing import Optional
from datetime import date
import bcrypt
import logging

logging.basicConfig(level=logging.INFO) #changes
logger = logging.getLogger(__name__)

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

try:
    client = bigquery.Client(project=PROJECT_ID)
except Exception as e:
    logger.critical(f"Failed to initialize BigQuery client: {e}")
    raise RuntimeError("Could not connect to BigQuery") from e
 
 
def run_query(query: str, params: list = []):
    """Run a parameterized BigQuery query and return rows as dicts."""
    try:
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        result = client.query(query, job_config=job_config).result()
        return [dict(row) for row in result]
    except BadRequest as e:
        logger.error(f"BigQuery bad request: {e}")
        raise HTTPException(status_code=400, detail="Malformed query or invalid parameters")
    except NotFound as e:
        logger.error(f"BigQuery resource not found: {e}")
        raise HTTPException(status_code=404, detail="Database table or resource not found")
    except GoogleAPIError as e:
        logger.error(f"BigQuery API error: {e}")
        raise HTTPException(status_code=503, detail="Database service temporarily unavailable")
    except Exception as e:
        logger.error(f"Unexpected database error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


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
    category: Optional[str]= None
    size: Optional[str] = None
    calories: Optional[int] = None
    price: Optional[float]= None

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
    drive_thru: Optional[bool]= None
    door_dash: Optional[bool]= None

class Order(BaseModel):
    hours: Optional[str] = None
    amenities: Optional[str] = None

class OrderItem(BaseModel):
    id: Optional[str]= None
    menu_item_id: str
    item_name: Optional[str]= None
    size: Optional[str] = None
    quantity: Optional[int] = None
    price: Optional[float]= None

class OrderHistory(BaseModel):
    order_id: str
    member_id: str
    store_id: str
    order_date: Optional[datetime]= None
    items_subtotal: Optional[float] = None
    order_discount: Optional[float] = None
    order_subtotal: Optional[float] = None
    sales_tax: Optional[float] = None
    order_total: Optional[float]= None
    items: list[OrderItem] = []

class PointsBalance(BaseModel):
    member_id: str
    total_points: int


@app.get("/")
def root():
    return {"message": "Uncle Joe's Coffee API is running"}

#User Authorization Endpoint 

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

@app.get("/locations/{id}", response_model=list[Location])
def get_ind_locations(id: str):
    query = f"""
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
        WHERE id = '{id}'
    """
    return run_query(query)

#Order History Endpoint

@app.get("/members/{member_id}/orders", response_model=list[OrderHistory])
def get_orders(member_id: str):
    query = """
        SELECT 
            o.order_id,
            o.member_id,   -- Added member_id to the SELECT
            o.store_id,
            o.order_date,
            o.order_total,
            ARRAY_AGG(STRUCT(
                oi.menu_item_id,
                oi.item_name,
                oi.quantity,
                oi.price
            )) as items
        FROM `mgmt545-groupproject.unlce_joes.orders` o
        JOIN `mgmt545-groupproject.unlce_joes.order_items` oi
            ON o.order_id = oi.order_id
        WHERE o.member_id = @member_id
        GROUP BY 
            o.order_id, 
            o.member_id,   -- Added member_id to GROUP BY
            o.store_id, 
            o.order_date, 
            o.order_total
        ORDER BY o.order_date DESC
    """
    
    params = [bigquery.ScalarQueryParameter("member_id", "STRING", member_id)]
    rows = run_query(query, params)
    
    return [OrderHistory(**dict(row)) for row in rows]

#Points Balance Endpoint

@app.get("/members/{member_id}/points", response_model=PointsBalance)
def get_points(member_id: str):
    query = """
        SELECT SUM(FLOOR(order_total)) as total_points
        FROM mgmt545-groupproject.unlce_joes.orders
        WHERE member_id = @member_id
    """
    params = [bigquery.ScalarQueryParameter("member_id", "STRING", member_id)]
    results = run_query(query, params)
    total = results[0]["total_points"] or 0
    return PointsBalance(member_id=member_id, total_points=int(total))


#Google Maps Endpoint
