# Run with: arcc run examples/demo.arc

create forced unique bucket Names (first_name: string, last_name: string);

truncate Names;

insert into Names (first_name: "Bob", last_name: "Marley", non_existent_field: "value", another_non_field: "yo");
insert into Names (first_name: "Charlie", last_name: "Parker");
insert into Names (first_name: "Charlie", last_name: "Brown");
insert into Names (first_name: "Billy", last_name: "Bobby");

commit!;

get * from Names where non_existent_field is null;
get * from Names where non_existent_field is not null;

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

create forced unique bucket Products (name: string, price: float, in_stock: bool);
truncate Products;

insert into Products (name: "Widget", price: 9.99, in_stock: true);
insert into Products (name: "Gadget", price: 24.99, in_stock: false);
insert into Products (name: "Doohickey", price: 4.49, in_stock: true);
insert into Products (name: "Doohickey2", price: 4.49, in_stock: true);

commit!;

delete from Products where name = "Doohickey";

get * from Products;
get * from Products where in_stock = true;
get name, price from Products where in_stock = true order by price desc;
get * from Products order by price asc;
get * from Products order by price desc;
get name, in_stock from Products where price > 3 and in_stock = true;
get name from Products where name like "D%" and price > 4 and in_stock = true;

describe Products;
describe Names;

get avg(price), median(price), min(price), max(price), stddev(price) from Products where in_stock = true;
get sum(price) from Products;
get count(*) from Products;

print "Above is the product count.";

export Products to csv("products.csv");

drop price from Products;

create forced unique bucket Products2 from csv("generated.csv");
get * from Products2;
