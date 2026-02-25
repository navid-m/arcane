# Arcane example script — run with: arcc run examples/demo.arc

# Create a bucket with a schema
create bucket Names (first_name: string, last_name: string);

insert into Names (first_name: "Bob", last_name: "Marley", non_existent_field: "value", another_non_field: "yo");
insert into Names ("Alice", "Cooper");
insert into Names ("Bob", "Marley");
insert into Names ("Alice", "Bentley");
insert into Names (first_name: "Charlie", last_name: "Parker");

# Get all rows
get * from Names;

# Get only the first 2 rows
get head(2) from Names;

# Get only the last 2 rows
get tail(2) from Names;

# Filter by field value
get * from Names where first_name = "Alice";

# Get hashes for all Alices
get __hash__ from Names where first_name = "Alice";

# Create another bucket
create bucket Products (name: string, price: float, in_stock: bool);

insert into Products (name: "Widget", price: 9.99, in_stock: true);
insert into Products (name: "Gadget", price: 24.99, in_stock: false);
insert into Products (name: "Doohickey", price: 4.49, in_stock: true);

get * from Products;
get * from Products where in_stock = true;
