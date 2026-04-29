from fastapi import FastAPI, HTTPException
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from pydantic import BaseModel
from typing import Optional, List
from datetime import date, datetime, timezone
import bcrypt

PROJECT_ID = "mgmt545-groupproject"
DATASET = "unlce_joes"


app = FastAPI(title="Uncle Joe's Coffee API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://uncle-joes-frontend-1055452553205.us-central1.run.app"],
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
    home_store: str

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

#classes for order button
class OrderCreationRequest(BaseModel):
    menu_item_id: str
    quantity: int

#retrieves request and creates the 
class OrderFullfilment(BaseModel):
    member_id: str
    store_id: str
    order_items: List[OrderCreation]


@app.get("/")
def root():
    return {"message": "Uncle Joe's Coffee API is running"}

#User Authorization Endpoint - Note that project name might need to be changed

@app.post("/login", response_model=LoginResponse)
def login(request: LoginRequest):
    query = """
        SELECT string_field_0 as id, string_field_1 || ' ' || string_field_2 as name, string_field_3 as email, string_field_5 as home_store, string_field_6 as password_hash
        FROM mgmt545-groupproject.unlce_joes.members     
        WHERE string_field_3 = @email
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


@app.get("/menu/{id}", response_model=MenuItem)
def get_menu_item(id: str):
    query = """
        SELECT id, name, category, size, calories, price
        FROM `mgmt545-groupproject.unlce_joes.menu`
        WHERE id = @id
    """
    params = [bigquery.ScalarQueryParameter("id", "STRING", id)]
    results = run_query(query, params)
    if not results:
        raise HTTPException(status_code=404, detail="Menu item not found")
    return results[0]

@app.post("/orders", status_code=201)
async def create_order(order: OrderFullfilment):

    # retrieve items in the order
    requested_item_ids = [item.menu_item_id for item in order.order_items]
    
    # Find the information to post the order from menu
    # we do this here so that customers cannot manipulate information
    query = """
        SELECT 
            menu_item_id, 
            item_name,
            price 
        FROM `your_project.your_dataset.menu`
        WHERE menu_item_id IN UNNEST(@item_ids)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("item_ids", "STRING", requested_item_ids)
        ]
    )
    

    try:
        query_job = bq_client.query(query, job_config=job_config)
        menu_results = query_job.result()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    menu_catalog = {
        row.menu_item_id: {"item_name": row.item_name, "price": row.price} 
        for row in menu_results
    }

    # Gather the max order_id so that the id is correctly ordered
    max_id_query = """
        SELECT MAX(CAST(order_id AS INT64)) as current_max 
        FROM `your_project.your_dataset.orders`
    """

    # Create new order_id by adding 1 to the previous order_id
    try:
        max_job = bq_client.query(max_id_query)
        max_result = list(max_job.result())
        current_max = max_result[0].current_max if max_result and max_result[0].current_max is not None else 0
        order_id = str(current_max + 1)
    except Exception as e:
        order_id = "1"


    order_total = 0.0
    order_item_rows = []

    # make sure the order is asking for an item that is currently offered
    for requested_item in order.order_items:
        if requested_item.menu_item_id not in menu_catalog:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid item requested: {requested_item.menu_item_id}"
            )
            
        # retrieve price and other important information to correctly update orders table 
        official_item = menu_catalog[requested_item.menu_item_id]
        item_price = official_item["price"]
        item_total = item_price * requested_item.quantity
        
        order_total += item_total
        
        order_item_rows.append({
            "order_id": order_id,
            "menu_item_id": requested_item.menu_item_id,
            "item_name": official_item["item_name"],
            "quantity": requested_item.quantity,
            "price": item_price 
        })

    # retrieve order time so that there is no null values in the posted order
    order_date = datetime.now(timezone.utc).isoformat()
    
    # create a dictionary containing all information for post
    order_row = {
        "order_id": order_id,
        "member_id": order.member_id,
        "store_id": order.store_id,
        "order_date": order_date,
        "order_total": order_total
    }


    # take dictionary and complete the post
    try:
        orders_table_ref = "your_project.your_dataset.orders"
        items_table_ref = "your_project.your_dataset.order_items"
        
        orders_errors = bq_client.insert_rows_json(orders_table_ref, [order_row])
        if orders_errors:
            raise Exception(f"Failed to insert order: {orders_errors}")
            
        items_errors = bq_client.insert_rows_json(items_table_ref, order_item_rows)
        if items_errors:
            raise Exception(f"Failed to insert order items: {items_errors}")

        return {
            "message": "Order successfully created", 
            "order_id": order_id,
            "order_total": order_total,
            "status": "success"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
