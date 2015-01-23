Gallus - The little ORM that could.
===================================

Gallus is a [Dapper](https://github.com/StackExchange/dapper-dot-net) inspired micro-ORM that follows the same ethos as Dapper but it supports nested collections out the box. This project is in no way intended to be a rival to Dapper it was written more as a learning exercise and out of sheer curiosity. It provides a very similar api to Dapper providing extension methods that hang off the connection object. It also provides some basic methods to aid with data access in general.

Thoughts on ORM's
==============

An ORM is a tool that is supposed to make your life easier, my experience using them has been that this is not always the case. They increase the complexity of your application by adding another layer to manage while offering mediocre performance (as described in [Sam Saffron](http://samsaffron.com/)'s post: "[How I learned to stop worrying and write my own ORM](http://samsaffron.com/archive/2011/03/30/How+I+learned+to+stop+worrying+and+write+my+own+ORM)"). There is an obsessive fixation with ORM's because they (the main mature players, NHibernate, EF etc) generate boilerplate CRUD SQL for you. As developers we want to be writing code and focusing on the problem domain and writing SQL is seen as an unnecessary  distraction from this, a hardship that ORM's seek to resolve. 

I believe this to be a misnomer, writing SQL is a relatively trivial exercise and whether you are writing SQL or writing code to map an object with an ORM you aren't really gaining a lot from that deal in terms of raw key strokes (although ORM's offer fringe benefits such as allowing your application to be database agnostic, table generation etc). I believe this benefit is ultimately cancelled out once you start to factor in the inevitable wrestling you'll need to do with clunky, bloated api's.

Once you accept that generating SQL is simply window dressing you quickly realise the real benefit and ultimately the real goal of an ORM - mapping. It's in the title after all, object relational __mapping__. Writing the SQL is trivial, laborious perhaps no doubt, but mapping each column to an object property is the real gruelling part of job. 

Dapper was written for a very specific purpose, to allow Stack Overflow hands-on control over areas of the system that were performance black holes where increased ORM expertise provided diminishing returns. One feature that I felt it lacked (although I completely understand why it wasn't included) was the ability to map nested collections, 1:N relationships. So in the spirit of Sam Saffron I decided to stop worrying and write my own ORM. I had no lofty intentions for it other than for it to be as simple as Dapper, reasonably fast and envisaged it perhaps become a tool for RAD/prototyping purposes.

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

The __Cars__ property has not been mapped and will rightly return null.

Query with join for nested collection:
--------------------------------------

```csharp
string sql = @"SELECT * FROM Person a
					INNER JOIN Car b ON a.Id = b.PersonId";

var people = connection.Query<Person, IList<Car>>(sql, (person, cars) => { person.Cars = cars; });
```

You can see the __Query__ method takes two generic type parameters, __Person__ and __IList<Car>__ which are then mapped in-line. In order to do this mapping Gallus makes two assumptions, the first being that each object has an __Id__ column as a primary key and that this is the same for the database tables underneath (there is an override *splitOn* if this is not the case that takes comma separated string) and the second being the objects will be joined in the same order that they are mapped, thats important.

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

For anyone familiar with Dapper the api should be self-explanatory. The dynamic property name needs to match the query parameter.

Executing SQL 
-------------

Gallus provides a method for executing raw SQL.

```csharp
connection.Exec("INSERT INTO....", new { id = 1, firstname = "Test", lastname = "user" });
```

Read
----

For when mapping is too complex for whatever reason Gallus provides a __Read__ method that executes your query SQL but bypasses the mapping and returns an IDataReader to allow you to manually map your object.

```csharp
IDataReader reader = connection.Read("SELECT * FROM....");
```

Gallus also provides a whole host of extension methods for the IDataReader to read specific data types:

```csharp
person.Id = reader.ReadInt32("Id");
person.FirstName = reader.ReadString("FirstName");
...
```		

Speed
-----

Benchmarking Gallus vs Dapper I'd say Dapper is marginally faster but not by a great deal. While writing Gallus I always had an eye on performance but in many ways it is a naïve implementation, my rationale was that if needed it could be optimised	at a later date. It is certainly no slouch though and I am very pleased with its performance so far.	

