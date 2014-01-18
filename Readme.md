Gallus - The little ORM that could.
===================================

Gallus is a Dapper inspired micro-ORM that follows the same ethos as Dapper but with a twist, it supports nested collections out the box. This project is in no way intended to be a rival to Dapper it was written more as a learning exercise and out of sheer curiosity. It provides a very similar api to Dapper with some bits missing and some bits added. As well as providing methods for mapping it also provides some basic methods to help with data access in general.

Mapping a query to an object
----------------------------

Like Dapper, Gallus is a single file that can be dropped into your project with all the methods hanging off the IDbConnection object as extension methods and assumes an open connection.

Basic object model:

```csharp
public class Person {
	public int Id { get; set; }
	public string FirstName { get; set; }
	public string LastName { get; set; }
	public IList<Car> Cars { get; set; }
}

public class Car {
	public int Id { get; set; }
	public string Make { get; set; }
	public string Model { get; set; }
}
```

Simple query:
-------------

```csharp
var people = connection.Query<Person>("SELECT * FROM PERSON"); 
```

The *Cars* property has not been mapped and will rightly return null.

Query with join for nested collection:
--------------------------------------

```csharp
string sql = @"SELECT * FROM Person a
					INNER JOIN Car b ON a.Id = b.PersonId";

var people = connection.Query<Person, IList<Car>>(sql, (person, cars) => { person.Cars = cars; });
```

You can see the **Query** method takes two generic type parameters, **Person** and **IList<Car>** which are then mapped in-line. In order to do this mapping Gallus makes two assumptions, the first being that each object has an **Id** column as a primary key and that this is the same for the database tables underneath (there is an override *splitOn* if this is not the case that takes comma seperated string) and the second being the objects will be joined in the same order that they are mapped, thats important.

In this way you can map single nested objects e.g. 

```csharp
var people = connection.Query<Person, Car>(sql, (person, car) => { person.Car = car; });
``` 

You can map upto 11 objects in total, one master object and ten nested objects/collections of objects e.g.

```csharp
var people = connection.Query<Person, IList<Car>, IList<Address>, BankAccount...>(sql, (person, cars, addresses, bankAccount...) => { 
	person.Cars = cars; 
	person.Addresses = addresses;
	person.BankAccount = bankAccount
	...
});
```

Passing parameters
------------------

Very simple:

```csharp
var people = connection.Query<Person>("SELECT * FROM PERSON WHERE Id = @id", new { id = 4 }); 
```

For anyone familiar with Dapper the api should be self explanitory.

Executing SQL 
-------------

```csharp
connection.Exec("INSERT INTO....", new { id = 1, firstname = "Test", lastname = "user" });
```

Read
----

For when mapping is too complex for whatever reason Gallus provides a **Read** method that executes your query sql but bypasses the mapping and returns an IDataReader to allow you to manually map your object.

```csharp
IDataReader reader = connection.Read("SELECT * FROM....");
```

Gallus also provides a whole host of extension methods for the IDataReader to read specific data types:

```csharp
person.Id = reader.ReadInt32("Id");
person.FirstName = reader.ReadString("FirstName");
...
```				

