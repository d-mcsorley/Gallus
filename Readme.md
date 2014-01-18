Gallus - The little ORM that could.
===================================

Gallus is a Dapper inspired micro-ORM that follows the same ethos as Dapper but with a twist, it supports nested collections out the box. This project is in no way intended to be a rival to Dapper it was written more as a learning exercise and out of sheer curiosity. It provides a very similar api to Dapper with some bits missing and some bits added. As well as providing methods for mapping it also provides some basic methods to help with data access in general.

Mapping a query to an object
----------------------------

Like Dapper all the methods hang off the IDbConnection object as extension methods and assumes an open connection.

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

Query with join for nested collection:
--------------------------------------

```csharp
var people2 = connection.Query<Person, IList<Car>>(sql, (person, cars) => { person.Cars = cars; });
```