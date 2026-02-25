create bucket TestBatch (name: string, age: int, city: string)

insert into TestBatch (name: "Alice", age: 30, city: "NYC")
insert into TestBatch ([name: "Bob", age: 25, city: "LA"], [name: "Charlie", age: 35, city: "SF"], [name: "Diana", age: 28, city: "Seattle"])

bulk {
    insert into TestBatch (name: "Eve", age: 32, city: "Boston")
    insert into TestBatch (name: "Frank", age: 29, city: "Austin")
    insert into TestBatch (name: "Grace", age: 31, city: "Denver")
}

get * from TestBatch
