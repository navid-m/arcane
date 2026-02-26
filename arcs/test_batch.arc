create forced unique bucket TestBatch2 (name: string, age: int, city: string);

insert into TestBatch2 (name: "Alice", age: 30, city: "NYC");
insert into TestBatch2 (
    [name: "Bob", age: 25, city: "LA"],
    [name: "Charlie", age: 35, city: "SF"],
    [name: "Diana", age: 28, city: "Seattle"]
);

delete from TestBatch2 where age >= 31;

set TestBatch2 ( name: "Eve", age: 22, city: "Chicago" ) where name = "Alice";
set TestBatch2 ( name: "Genevra" ) where name = "Eve";

commit!;

get * from TestBatch2;
