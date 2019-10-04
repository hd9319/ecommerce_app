CREATE_INVENTORY_TABLE = """
                CREATE TABLE Inventory (
                        id int NOT NULL PRIMARY KEY AUTO_INCREMENT,
                        Brand varchar(255) NOT NULL,
                        PartNumber varchar(255) NOT NULL,
                        Quantity varchar(255),
                        Discontinued varchar(255)
                        Source varchar(255),
                        CreatedOn datetime DEFAULT CURRENT_TIMESTAMP
                        )
"""

TRUNCATE_SQL = """
    TRUNCATE TABLE [%s].Inventory;
    """
      
INSERT_SQL = """
INSERT INTO [%s].Inventory (%s)
VALUES (%s)
"""

UPDATE_INVENTORY = """
UPDATE [%s].Electronics AS e
INNER JOIN [%s].Inventory AS inv 
ON e.Brand = inv.Brand AND e.PartNumber = inv.PartNumber
SET
    e.Quantity = b.Quantity
"""

