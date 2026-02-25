create bucket TestBatch2 (name: string, age: int, city: string);

insert into TestBatch2 (name: "Alice", age: 30, city: "NYC");
insert into TestBatch2 (
    [name: "Bob", age: 25, city: "LA"],
    [name: "Charlie", age: 35, city: "SF"],
    [name: "Diana", age: 28, city: "Seattle"]
);

get * from TestBatch2;
