CREATE_PRODUCT_TABLE = """
                CREATE TABLE Electronics (
                        id int NOT NULL PRIMARY KEY AUTO_INCREMENT,
                        Brand varchar(255) NOT NULL,
                        PartNumber varchar(255) NOT NULL,
                        Category varchar(255),
                        ImageUrl varchar(255),
                        RegularPrice float,
                        SalePrice float,
                        Description varchar(2000),
                        CustomerRating float,
                        CustomerRatingCount int,
                        CustomerReviewCount int,
                        SourceUrl varchar(255),
                        Quantity int DEFAULT 99999,
                        CreatedOn datetime DEFAULT CURRENT_TIMESTAMP
                        )
"""

CREATE_MANUFACTURER_TABLE = """
    CREATE TABLE Brands (
        BrandID int NOT NULL PRIMARY KEY auto_increment,
        Name varchar(255) NOT NULL,
        Description varchar(1000),
        SupplierSource varchar(255),
        Published bit DEFAULT 0,
        CreatedOn datetime DEFAULT current_timestamp
    )
"""

TRUNCATE_SQL = """
    TRUNCATE TABLE [%s].Electronics;
    """
    
    
INSERT_SQL = """
INSERT INTO [%s].Electronics (%s)
VALUES (%s)
"""

INSERT_NEW_BRANDS_SQL = """
INSERT INTO [%s].Brands (Name, SupplierSource)
SELECT DISTINCT Brand, 'BB'
    FROM [%s].Electronics
"""

UPDATE_SQL = """
INSERT INTO [%s].Electronics (%s)
VALUES (%s)
"""

