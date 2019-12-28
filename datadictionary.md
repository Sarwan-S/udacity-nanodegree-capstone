# Data Dictionaries

## 1. Staging Files

#### Iowa_Liquor_Sales_noheaders.csv

| Column                | Data Type | Description                                                                                                                                                                                                                                                |
|-----------------------|-----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Invoice/Item Number   | string    | Concatenated invoice and line number associated with the liquor order. This provides a unique identifier for the individual liquor products included in the store order                                                                                    |
| Date                  | date      | Date of order                                                                                                                                                                                                                                              |
| Store Number          | double    | Unique number assigned to the store who ordered the liquor.                                                                                                                                                                                                |
| Store Name            | string    | Name of store who ordered the liquor.                                                                                                                                                                                                                      |
| Address               | string    | Address of store who ordered the liquor.                                                                                                                                                                                                                   |
| City                  | string    | City where the store who ordered the liquor is located                                                                                                                                                                                                     |
| Zip Code              | integer   | Zip code where the store who ordered the liquor is located                                                                                                                                                                                                 |
| Store Location        | string    | Location of store who ordered the liquor. The Address, City, State and Zip Code are geocoded to provide geographic coordinates. Accuracy of geocoding is dependent on how well the address is interpreted and the completeness of the reference data used. |
| County Number         | double    | Iowa county number for the county where store who ordered the liquor is located                                                                                                                                                                            |
| County                | string    | County where the store who ordered the liquor is located                                                                                                                                                                                                   |
| Category              | double    | Category code associated with the liquor ordered                                                                                                                                                                                                           |
| Category Name         | string    | Category of the liquor ordered.                                                                                                                                                                                                                            |
| Vendor Number         | double    | The vendor number of the company for the brand of liquor ordered                                                                                                                                                                                           |
| Vendor Name           | string    | The vendor name of the company for the brand of liquor ordered                                                                                                                                                                                             |
| Item Number           | double    | Item number for the individual liquor product ordered.                                                                                                                                                                                                     |
| Item Description      | string    | Description of the individual liquor product ordered.                                                                                                                                                                                                      |
| Pack                  | integer   | The number of bottles in a case for the liquor ordered                                                                                                                                                                                                     |
| Bottle Volume (ml)    | integer   | Volume of each liquor bottle ordered in milliliters.                                                                                                                                                                                                       |
| State Bottle Cost     | decimal   | The amount that Alcoholic Beverages Division paid for each bottle of liquor ordered                                                                                                                                                                        |
| State Bottle Retail   | decimal   | The amount the store paid for each bottle of liquor ordered                                                                                                                                                                                                |
| Bottles Sold          | integer   | The number of bottles of liquor ordered by the store                                                                                                                                                                                                       |
| Sale (Dollars)        | decimal   | Total cost of liquor order (number of bottles multiplied by the state bottle retail)                                                                                                                                                                       |
| Volume Sold (Liters)  | decimal   | Total volume of liquor ordered in liters. (i.e. (Bottle Volume (ml) x Bottles Sold)/1,000)                                                                                                                                                                 |
| Volume Sold (Gallons) | decimal   | Total volume of liquor ordered in gallons. (i.e. (Bottle Volume (ml) x Bottles Sold)/3785.411784)                                                                                                                                                          |

#### usholidays.json

| Column  | Data Type | Description        |
|---------|-----------|--------------------|
| Date    | date      | Date of US holiday |
| Holiday | string    | Name of US holiday |

#### weather/*.csv

| Column                | Data Type | Description                                                        |
|-----------------------|-----------|--------------------------------------------------------------------|
| County                | string    | Name of Iowa county                                                |
| State                 | string    | Name of Iowa state - Defaulted to 'Iowa'                           |
| Average Temperature   | double    | Average temperature for that month for that county                 |
| Latitude (Generated)  | double    | Generated latitude for that county where temperature was recorded  |
| Longitude (Generated) | double    | Generated longitude for that county where temperature was recorded |
| Year                  | integer   | Year of data                                                       |
| Month                 | integer   | Month of data                                                      |

## 2. Fact Table

#### liquor_sales

| Column             | Data Type | Description                                                                                                                                                             |
|--------------------|-----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| invoice_number     | string    | Concatenated invoice and line number associated with the liquor order. This provides a unique identifier for the individual liquor products included in the store order |
| sales_date         | date      | Date of order                                                                                                                                                           |
| store_number       | double    | Unique number assigned to the store who ordered the liquor.                                                                                                             |
| county_number      | double    | Iowa county number for the county where store who ordered the liquor is located                                                                                         |
| item_number        | double    | Item number for the individual liquor product ordered.                                                                                                                  |
| vendor_number      | double    | The vendor number of the company for the brand of liquor ordered                                                                                                        |
| bottles_sold       | integer   | The number of bottles of liquor ordered by the store                                                                                                                    |
| volume_sold_litres | decimal   | Total volume of liquor ordered in liters. (i.e. (Bottle Volume (ml) x Bottles Sold)/1,000)                                                                              |
| item_cost_price    | decimal   | The amount that Alcoholic Beverages Division paid for each bottle of liquor ordered                                                                                     |
| item_retail_price  | decimal   | The amount the store paid for each bottle of liquor ordered                                                                                                             |
| sales_usd          | decimal   | Total cost of liquor order (number of bottles multiplied by the state bottle retail)                                                                                    |
| climate_temp       | double    | Average temperature in the county during the purchase                                                                                                                   |

## 3. Dimension Tables

#### items

| Column        | Data Type | Description                                            |
|---------------|-----------|--------------------------------------------------------|
| item_number   | double    | Item number for the individual liquor product ordered. |
| description   | string    | Description of the individual liquor product ordered.  |
| category_name | string    | Category of the liquor ordered.                        |
| bottle_volume | integer   | Volume of each liquor bottle ordered in milliliters.   |
| pack          | integer   | The number of bottles in a case for the liquor ordered |

#### vendors

| Column        | Data Type | Description                                                      |
|---------------|-----------|------------------------------------------------------------------|
| vendor_number | double    | The vendor number of the company for the brand of liquor ordered |
| vendor_name   | string    | The vendor name of the company for the brand of liquor ordered   |

#### counties

| Column        | Data Type | Description                                                                     |
|---------------|-----------|---------------------------------------------------------------------------------|
| county_number | double    | Iowa county number for the county where store who ordered the liquor is located |
| county        | string    | County where the store who ordered the liquor is located                        |

#### stores

| Column       | Data Type | Description                                                 |
|--------------|-----------|-------------------------------------------------------------|
| store_number | double    | Unique number assigned to the store who ordered the liquor. |
| store_name   | string    | Name of store who ordered the liquor.                       |
| address      | string    | Address of store who ordered the liquor.                    |
| city         | string    | City where the store who ordered the liquor is located      |
| zipcode      | integer   | Zip code where the store who ordered the liquor is located  |
| latitude     | string    | Geographic latitude coordinates of the store's location     |
| longitude    | string    | Geographic longitude coordinates of the store's location    |

#### time

| Column       | Data Type | Description                                                               |
|--------------|-----------|---------------------------------------------------------------------------|
| sales_date   | date      | Date of order                                                             |
| day          | integer   | Day of order                                                              |
| week         | integer   | Week of order                                                             |
| month        | integer   | Month of order                                                            |
| year         | integer   | Year of order                                                             |
| weekday      | integer   | Weekday of order                                                          |
| is_holiday   | boolean   | True or False indicator of whether the date of order fell on a US holiday |
| holiday_name | string    | Name of the holiday where the order fell on                               |