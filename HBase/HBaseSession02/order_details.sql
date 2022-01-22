CREATE TABLE IF NOT EXISTS order_details (
      orderID VARCHAR NOT NULL,
      productID VARCHAR,
      userID VARCHAR,
      CONSTRAINT pk
      PRIMARY KEY (orderID));
