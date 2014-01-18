using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Gallus {
	public static class Gallus {
		public static int Exec (this IDbConnection connection, string sql, dynamic parameters = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType commandType = CommandType.Text) {
			IDbCommand command = GetCommand(connection, sql, parameters, transaction, commandTimeout, commandType);
			return command.ExecuteNonQuery();
		}

		public static IDataReader Read (this IDbConnection connection, string sql, dynamic parameters = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType commandType = CommandType.Text) {
			IDbCommand command = GetCommand(connection, sql, parameters, transaction, commandTimeout, commandType);
			return command.ExecuteReader();
		}

		public static IDbCommand GetCommand (this IDbConnection connection, string sql, dynamic parameters = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType commandType = CommandType.Text) {
			IDbCommand command = connection.CreateCommand();
			command.CommandText = sql;
			command.CommandType = commandType;
			command.Transaction = transaction;

			if (commandTimeout.HasValue)
				command.CommandTimeout = commandTimeout.Value;

			if (parameters != null) {
				PropertyInfo[] propertyInfos = parameters.GetType().GetProperties();

				foreach (PropertyInfo property in propertyInfos) {
					IDbDataParameter parameter = command.CreateParameter();
					parameter.ParameterName = property.Name;
					parameter.Value = property.GetValue(parameters, null);
					parameter.DbType = TypeMap[property.PropertyType];

					if (parameter.Value == null)
						parameter.Value = DBNull.Value;

					command.Parameters.Add(parameter);
				}
			}
			return command;
		}

		public static IDbCommand GetCommand (this IDbConnection connection, string sql, IList<IDbDataParameter> parameters = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType commandType = CommandType.Text) {
			IDbCommand command = connection.CreateCommand();
			command.CommandText = sql;
			command.CommandType = commandType;
			command.Transaction = transaction;

			if (commandTimeout.HasValue)
				command.CommandTimeout = commandTimeout.Value;

			if (parameters != null) {
				foreach (IDbDataParameter parameter in parameters) {
					if (parameter.Value == null)
						parameter.Value = DBNull.Value;

					command.Parameters.Add(parameter);
				}
			}
			return command;
		}

		public static void Insert (this IDbConnection connection, string tableName, dynamic parameters, IDbTransaction transaction = null) {
			IDbCommand command = GetCommand(connection, "", parameters, transaction, null, CommandType.Text);

			StringBuilder columnsBuilder = new StringBuilder();
			StringBuilder valuesBuilder = new StringBuilder();

			foreach (IDbDataParameter dbParam in command.Parameters) {
				columnsBuilder.Append(dbParam.ParameterName);
				columnsBuilder.Append(", ");
				valuesBuilder.Append(GetParameterName(dbParam.ParameterName));
				valuesBuilder.Append(", ");
			}

			columnsBuilder.Remove(columnsBuilder.Length - 2, 2);
			valuesBuilder.Remove(valuesBuilder.Length - 2, 2);

			string sql = String.Format(@"INSERT INTO {0} ({1}) VALUES ({2})", tableName, columnsBuilder.ToString(), valuesBuilder.ToString());
			command.CommandText = sql;
			command.ExecuteNonQuery();
		}

		private static string GetParameterName (string parameterName) {
			return "@" + parameterName;
		}

		public static IEnumerable<TFirst> Query<TFirst> (this IDbConnection connection, string sql, dynamic parameters = null, IDbTransaction transaction = null, string splitOn = "id", int? commandTimeout = null, CommandType commandType = CommandType.Text) {
			using (IDataReader reader = Read(connection, sql, parameters, transaction, commandTimeout, commandType))
				return new Mapper<TFirst, DoNotMap, DoNotMap, DoNotMap, DoNotMap, DoNotMap, DoNotMap, DoNotMap, DoNotMap, DoNotMap, DoNotMap>().MapFromReader(reader, null, splitOn);
		}

		public static IEnumerable<TFirst> Query<TFirst, TSecond> (this IDbConnection connection, string sql, Action<TFirst, TSecond> map, dynamic parameters = null, IDbTransaction transaction = null, string splitOn = "id", int? commandTimeout = null, CommandType commandType = CommandType.Text) {
			using (IDataReader reader = Read(connection, sql, parameters, transaction, commandTimeout, commandType))
				return new Mapper<TFirst, TSecond, DoNotMap, DoNotMap, DoNotMap, DoNotMap, DoNotMap, DoNotMap, DoNotMap, DoNotMap, DoNotMap>().MapFromReader(reader, map, splitOn);
		}

		public static IEnumerable<TFirst> Query<TFirst, TSecond, TThird> (this IDbConnection connection, string sql, Action<TFirst, TSecond, TThird> map, dynamic parameters = null, IDbTransaction transaction = null, string splitOn = "id", int? commandTimeout = null, CommandType commandType = CommandType.Text) {
			using (IDataReader reader = Read(connection, sql, parameters, transaction, commandTimeout, commandType))
				return new Mapper<TFirst, TSecond, TThird, DoNotMap, DoNotMap, DoNotMap, DoNotMap, DoNotMap, DoNotMap, DoNotMap, DoNotMap>().MapFromReader(reader, map, splitOn);
		}

		public static IEnumerable<TFirst> Query<TFirst, TSecond, TThird, TFourth> (this IDbConnection connection, string sql, Action<TFirst, TSecond, TThird, TFourth> map, dynamic parameters = null, IDbTransaction transaction = null, string splitOn = "id", int? commandTimeout = null, CommandType commandType = CommandType.Text) {
			using (IDataReader reader = Read(connection, sql, parameters, transaction, commandTimeout, commandType))
				return new Mapper<TFirst, TSecond, TThird, TFourth, DoNotMap, DoNotMap, DoNotMap, DoNotMap, DoNotMap, DoNotMap, DoNotMap>().MapFromReader(reader, map, splitOn);
		}

		public static IEnumerable<TFirst> Query<TFirst, TSecond, TThird, TFourth, TFifth> (this IDbConnection connection, string sql, Action<TFirst, TSecond, TThird, TFourth, TFifth> map, dynamic parameters = null, IDbTransaction transaction = null, string splitOn = "id", int? commandTimeout = null, CommandType commandType = CommandType.Text) {
			using (IDataReader reader = Read(connection, sql, parameters, transaction, commandTimeout, commandType))
				return new Mapper<TFirst, TSecond, TThird, TFourth, TFifth, DoNotMap, DoNotMap, DoNotMap, DoNotMap, DoNotMap, DoNotMap>().MapFromReader(reader, map, splitOn);
		}

		public static IEnumerable<TFirst> Query<TFirst, TSecond, TThird, TFourth, TFifth, TSixth> (this IDbConnection connection, string sql, Action<TFirst, TSecond, TThird, TFourth, TFifth, TSixth> map, dynamic parameters = null, IDbTransaction transaction = null, string splitOn = "id", int? commandTimeout = null, CommandType commandType = CommandType.Text) {
			using (IDataReader reader = Read(connection, sql, parameters, transaction, commandTimeout, commandType))
				return new Mapper<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, DoNotMap, DoNotMap, DoNotMap, DoNotMap, DoNotMap>().MapFromReader(reader, map, splitOn);
		}

		public static IEnumerable<TFirst> Query<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh> (this IDbConnection connection, string sql, Action<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh> map, dynamic parameters = null, IDbTransaction transaction = null, string splitOn = "id", int? commandTimeout = null, CommandType commandType = CommandType.Text) {
			using (IDataReader reader = Read(connection, sql, parameters, transaction, commandTimeout, commandType))
				return new Mapper<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, DoNotMap, DoNotMap, DoNotMap, DoNotMap>().MapFromReader(reader, map, splitOn);
		}

		public static IEnumerable<TFirst> Query<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TEighth> (this IDbConnection connection, string sql, Action<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TEighth> map, dynamic parameters = null, IDbTransaction transaction = null, string splitOn = "id", int? commandTimeout = null, CommandType commandType = CommandType.Text) {
			using (IDataReader reader = Read(connection, sql, parameters, transaction, commandTimeout, commandType))
				return new Mapper<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TEighth, DoNotMap, DoNotMap, DoNotMap>().MapFromReader(reader, map, splitOn);
		}

		public static IEnumerable<TFirst> Query<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TEighth, TNinth> (this IDbConnection connection, string sql, Action<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TEighth, TNinth> map, dynamic parameters = null, IDbTransaction transaction = null, string splitOn = "id", int? commandTimeout = null, CommandType commandType = CommandType.Text) {
			using (IDataReader reader = Read(connection, sql, parameters, transaction, commandTimeout, commandType))
				return new Mapper<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TEighth, TNinth, DoNotMap, DoNotMap>().MapFromReader(reader, map, splitOn);
		}

		public static IEnumerable<TFirst> Query<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TEighth, TNinth, TTenth> (this IDbConnection connection, string sql, Action<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TEighth, TNinth, TTenth> map, dynamic parameters = null, IDbTransaction transaction = null, string splitOn = "id", int? commandTimeout = null, CommandType commandType = CommandType.Text) {
			using (IDataReader reader = Read(connection, sql, parameters, transaction, commandTimeout, commandType))
				return new Mapper<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TEighth, TNinth, TTenth, DoNotMap>().MapFromReader(reader, map, splitOn);
		}

		public static IEnumerable<TFirst> Query<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TEighth, TNinth, TTenth, TEleventh> (this IDbConnection connection, string sql, Action<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TEighth, TNinth, TTenth, TEleventh> map, dynamic parameters = null, IDbTransaction transaction = null, string splitOn = "id", int? commandTimeout = null, CommandType commandType = CommandType.Text) {
			using (IDataReader reader = Read(connection, sql, parameters, transaction, commandTimeout, commandType))
				return new Mapper<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TEighth, TNinth, TTenth, TEleventh>().MapFromReader(reader, map, splitOn);
		}

		/// <summary>
		/// This is the class that does all the work for mapping a reader to a group of objects (thats a lie a tiny bit is delegated to the ClassMap class). It's not particulary
		/// clever nor was it designed to be, it simply creates the mapping classes and structures for holding the resolved objects and then traverses the reader creating objects,
		/// populating them and storing them to be reconstituted into a full object graph. If performance or memory becomes an issue then this can be refactored.
		/// </summary>
		private class Mapper<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TEighth, TNinth, TTenth, TEleventh> {
			private List<ClassMap> _classMaps;
			private List<DoubleKeyDictionary<string, string, object>> _objectDictionaries;
			List<TFirst> mappedObjects = new List<TFirst>();

			public Mapper () {
				_classMaps = new List<ClassMap>();
				_classMaps.Add(GetClassMap<TFirst>());
				_classMaps.Add(GetClassMap<TSecond>());
				_classMaps.Add(GetClassMap<TThird>());
				_classMaps.Add(GetClassMap<TFourth>());
				_classMaps.Add(GetClassMap<TFifth>());
				_classMaps.Add(GetClassMap<TSixth>());
				_classMaps.Add(GetClassMap<TSeventh>());
				_classMaps.Add(GetClassMap<TEighth>());
				_classMaps.Add(GetClassMap<TNinth>());
				_classMaps.Add(GetClassMap<TTenth>());
				_classMaps.Add(GetClassMap<TEleventh>());
				_classMaps.RemoveAll(x => { return x.MappedType == typeof(DoNotMap); });

				_objectDictionaries = new List<DoubleKeyDictionary<string, string, object>>();
				for (int i = 1; i <= _classMaps.Count; i++) {
					_objectDictionaries.Add(new DoubleKeyDictionary<string, string, object>());
				}
			}

			public IEnumerable<TFirst> MapFromReader (IDataReader reader, object map, string splitOn) {
				ColumnMap columnMap = new ColumnMap(reader, splitOn);

				while (reader.Read()) {
					string primaryObjectId = "";

					for (int i = 0; i < _classMaps.Count; i++) {
						string objectId = reader.GetValue(columnMap[i].ElementAt(0).Key).ToString();

						if (String.IsNullOrEmpty(objectId)) continue;

						primaryObjectId = i == 0 ? objectId : primaryObjectId;

						if (!_objectDictionaries[i].ContainsKey(primaryObjectId, objectId)) {
							object obj = _classMaps[i].MapFromReader(reader, columnMap[i]);
							_objectDictionaries[i].Add(primaryObjectId, objectId, obj);
						}
					}
				}

				foreach (string key in _objectDictionaries[0].Keys) {
					List<object> tempObjects = new List<object>();

					tempObjects.Add(_objectDictionaries[0][key][key]);

					for (int i = 1; i < _objectDictionaries.Count; i++) {
						if (_objectDictionaries[i].ContainsKey(key)) {
							if (_classMaps[i].IsCollection) {
								tempObjects.Add(CreateCollectionFromType(_classMaps[i].CollectionType, _objectDictionaries[i][key]));
							} else {
								tempObjects.Add(_objectDictionaries[i][key].ElementAt(0).Value);
							}
						} else {
							tempObjects.Add(null);
						}
					}

					switch (tempObjects.Count) {
						case 2:
							((Action<TFirst, TSecond>)map)((TFirst)tempObjects[0], (TSecond)tempObjects[1]);
							break;
						case 3:
							((Action<TFirst, TSecond, TThird>)map)((TFirst)tempObjects[0], (TSecond)tempObjects[1], (TThird)tempObjects[2]);
							break;
						case 4:
							((Action<TFirst, TSecond, TThird, TFourth>)map)((TFirst)tempObjects[0], (TSecond)tempObjects[1], (TThird)tempObjects[2], (TFourth)tempObjects[3]);
							break;
						case 5:
							((Action<TFirst, TSecond, TThird, TFourth, TFifth>)map)((TFirst)tempObjects[0], (TSecond)tempObjects[1], (TThird)tempObjects[2], (TFourth)tempObjects[3], (TFifth)tempObjects[4]);
							break;
						case 6:
							((Action<TFirst, TSecond, TThird, TFourth, TFifth, TSixth>)map)((TFirst)tempObjects[0], (TSecond)tempObjects[1], (TThird)tempObjects[2], (TFourth)tempObjects[3], (TFifth)tempObjects[4], (TSixth)tempObjects[5]);
							break;
						case 7:
							((Action<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh>)map)((TFirst)tempObjects[0], (TSecond)tempObjects[1], (TThird)tempObjects[2], (TFourth)tempObjects[3], (TFifth)tempObjects[4], (TSixth)tempObjects[5], (TSeventh)tempObjects[6]);
							break;
						case 8:
							((Action<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TEighth>)map)((TFirst)tempObjects[0], (TSecond)tempObjects[1], (TThird)tempObjects[2], (TFourth)tempObjects[3], (TFifth)tempObjects[4], (TSixth)tempObjects[5], (TSeventh)tempObjects[6], (TEighth)tempObjects[7]);
							break;
						case 9:
							((Action<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TEighth, TNinth>)map)((TFirst)tempObjects[0], (TSecond)tempObjects[1], (TThird)tempObjects[2], (TFourth)tempObjects[3], (TFifth)tempObjects[4], (TSixth)tempObjects[5], (TSeventh)tempObjects[6], (TEighth)tempObjects[7], (TNinth)tempObjects[8]);
							break;
						case 10:
							((Action<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TEighth, TNinth, TTenth>)map)((TFirst)tempObjects[0], (TSecond)tempObjects[1], (TThird)tempObjects[2], (TFourth)tempObjects[3], (TFifth)tempObjects[4], (TSixth)tempObjects[5], (TSeventh)tempObjects[6], (TEighth)tempObjects[7], (TNinth)tempObjects[8], (TTenth)tempObjects[9]);
							break;
						case 11:
							((Action<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TEighth, TNinth, TTenth, TEleventh>)map)((TFirst)tempObjects[0], (TSecond)tempObjects[1], (TThird)tempObjects[2], (TFourth)tempObjects[3], (TFifth)tempObjects[4], (TSixth)tempObjects[5], (TSeventh)tempObjects[6], (TEighth)tempObjects[7], (TNinth)tempObjects[8], (TTenth)tempObjects[9], (TEleventh)tempObjects[10]);
							break;
					}
					mappedObjects.Add((TFirst)tempObjects[0]);
				}
				return mappedObjects;
			}
		}

		public class DoNotMap { }

		private static object CreateCollectionFromType (Type collectionType, Dictionary<string, object> collectionObjects) {
			if (collectionType.IsInterface) {
				if (collectionType.Name == typeof(IList<>).Name ||
					collectionType.Name == typeof(ICollection<>).Name ||
					collectionType.Name == typeof(IEnumerable<>).Name) {

					Type type = collectionType.GetGenericArguments()[0];
					Type customList = typeof(List<>).MakeGenericType(type);
					IList objectList = (IList)Activator.CreateInstance(customList);

					foreach (string key in collectionObjects.Keys) {
						objectList.Add(collectionObjects[key]);
					}
					return objectList;
				}
			}

			if (collectionType.IsArray) {
				object collection = Activator.CreateInstance(collectionType, new object[] { collectionObjects.Count });

				MethodInfo[] methods = collectionType.GetMethods();
				MethodInfo method = collectionType.GetMethods().FirstOrDefault(x => x.Name == "SetValue" && x.GetParameters().Count() == 2);

				for (int i = 0; i < collectionObjects.Count; i++) {
					method.Invoke(collection, new object[] { collectionObjects.ElementAt(i).Value, i });
				}
				return collection;
			}

			if (collectionType.IsGenericType) {
				Type type = collectionType.GetGenericArguments()[0];
				MethodInfo addMethod = collectionType.GetMethods().FirstOrDefault(x => x.Name == "Add" && x.GetParameters().Count() == 1 && x.GetParameters().FirstOrDefault(y => y.ParameterType == type) != null);

				if (addMethod != null) {
					object collection = Activator.CreateInstance(collectionType);

					foreach (string key in collectionObjects.Keys) {
						addMethod.Invoke(collection, new object[] { collectionObjects[key] });
					}

					return collection;
				}
			}
			return null;
		}

		private static ConcurrentDictionary<Type, ClassMap> _classMapDictionary = new ConcurrentDictionary<Type, ClassMap>();

		private static ClassMap<T> GetClassMap<T> () {
			Type type = typeof(T);
			if (_classMapDictionary.ContainsKey(type)) {
				return _classMapDictionary[type] as ClassMap<T>;
			}
			ClassMap<T> classMap = new ClassMap<T>();
			_classMapDictionary.TryAdd(type, classMap);
			return classMap;
		}

		private static ReaderWriterLock cachelock = new ReaderWriterLock();

		private static Dictionary<Type, DbType> _typeMap;

		private static Dictionary<Type, DbType> TypeMap {
			get {
				if (_typeMap == null) {
					try {
						cachelock.AcquireWriterLock(-1);
						_typeMap = new Dictionary<Type, DbType>();
						_typeMap[typeof(byte)] = DbType.Byte;
						_typeMap[typeof(sbyte)] = DbType.SByte;
						_typeMap[typeof(short)] = DbType.Int16;
						_typeMap[typeof(ushort)] = DbType.UInt16;
						_typeMap[typeof(int)] = DbType.Int32;
						_typeMap[typeof(uint)] = DbType.UInt32;
						_typeMap[typeof(long)] = DbType.Int64;
						_typeMap[typeof(ulong)] = DbType.UInt64;
						_typeMap[typeof(float)] = DbType.Single;
						_typeMap[typeof(double)] = DbType.Double;
						_typeMap[typeof(decimal)] = DbType.Decimal;
						_typeMap[typeof(bool)] = DbType.Boolean;
						_typeMap[typeof(string)] = DbType.String;
						_typeMap[typeof(char)] = DbType.StringFixedLength;
						_typeMap[typeof(Guid)] = DbType.Guid;
						_typeMap[typeof(DateTime)] = DbType.DateTime;
						_typeMap[typeof(DateTimeOffset)] = DbType.DateTimeOffset;
						_typeMap[typeof(TimeSpan)] = DbType.Time;
						_typeMap[typeof(byte[])] = DbType.Binary;
						_typeMap[typeof(byte?)] = DbType.Byte;
						_typeMap[typeof(sbyte?)] = DbType.SByte;
						_typeMap[typeof(short?)] = DbType.Int16;
						_typeMap[typeof(ushort?)] = DbType.UInt16;
						_typeMap[typeof(int?)] = DbType.Int32;
						_typeMap[typeof(uint?)] = DbType.UInt32;
						_typeMap[typeof(long?)] = DbType.Int64;
						_typeMap[typeof(ulong?)] = DbType.UInt64;
						_typeMap[typeof(float?)] = DbType.Single;
						_typeMap[typeof(double?)] = DbType.Double;
						_typeMap[typeof(decimal?)] = DbType.Decimal;
						_typeMap[typeof(bool?)] = DbType.Boolean;
						_typeMap[typeof(char?)] = DbType.StringFixedLength;
						_typeMap[typeof(Guid?)] = DbType.Guid;
						_typeMap[typeof(DateTime?)] = DbType.DateTime;
						_typeMap[typeof(DateTimeOffset?)] = DbType.DateTimeOffset;
						_typeMap[typeof(TimeSpan?)] = DbType.Time;
						_typeMap[typeof(Object)] = DbType.Object;
					} finally {
						cachelock.ReleaseWriterLock();
					}
				}
				return _typeMap;
			}
		}

		private class ClassMap {
			protected Type _mappedType;
			protected Type _collectionType;
			protected bool _isCollection;
			protected Dictionary<string, Type> _classPropertyTypes;
			protected Dictionary<string, PropertyInfo> _classProperties;

			public Type MappedType {
				get { return _mappedType; }
			}

			public Type CollectionType {
				get { return _collectionType; }
			}

			public bool IsCollection {
				get { return _isCollection; }
				set { _isCollection = value; }
			}

			public Dictionary<string, Type> ClassPropertyTypes {
				get { return _classPropertyTypes; }
				set { _classPropertyTypes = value; }
			}

			public object CreateMappedObject () {
				return Activator.CreateInstance(this.MappedType);
			}

			public object MapFromReader (IDataReader reader, Dictionary<int, string> columnMapping) {
				object obj = this.CreateMappedObject();

				foreach (int index in columnMapping.Keys) {
					if (this.ClassPropertyTypes.ContainsKey(columnMapping[index])) {
						_classProperties[columnMapping[index]].SetValue(obj, reader.ReadValue(index, ClassPropertyTypes[columnMapping[index]]), null);
					}
				}
				return obj;
			}
		}

		private class ClassMap<T> : ClassMap {
			public ClassMap () {
				_classPropertyTypes = new Dictionary<string, Type>();
				_classProperties = new Dictionary<string, PropertyInfo>();

				Type type = typeof(T);

				if (type.Name == typeof(IList<>).Name ||
						   type.Name == typeof(ICollection<>).Name ||
						   type.Name == typeof(IEnumerable<>).Name ||
						   type.Name == typeof(List<>).Name) {

					_collectionType = typeof(T);
					_isCollection = true;

					type = type.GetGenericArguments()[0];
				}

				if (type.IsArray) {
					_collectionType = type;
					_isCollection = true;

					type = type.GetElementType();
				}

				PropertyInfo[] propertyInfos = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

				foreach (PropertyInfo propertyInfo in propertyInfos) {
					if (!propertyInfo.CanWrite) continue;

					if (propertyInfo.PropertyType.IsClass && propertyInfo.PropertyType != typeof(string) && propertyInfo.PropertyType != typeof(byte[])) continue;

					if (propertyInfo.PropertyType.IsAbstract || propertyInfo.PropertyType.IsInterface) continue;

					_classPropertyTypes.Add(propertyInfo.Name, propertyInfo.PropertyType);
					_classProperties.Add(propertyInfo.Name, propertyInfo);
				}
				_mappedType = type;
			}
		}

		private class ColumnMap {
			private Dictionary<int, Dictionary<int, string>> _columnMap;

			public Dictionary<int, string> this[int key] {
				get { return _columnMap[key]; }
			}

			public ColumnMap (IDataReader reader, string splitOn) {
				_columnMap = new Dictionary<int, Dictionary<int, string>>();

				int objectCount = -1;
				for (int i = 0; i < reader.FieldCount; i++) {

					string columnName = reader.GetName(i);

					// Gallus will ignore any leading columns until it encounters a genuine identifier field.
					if (splitOn.ToLower().IndexOf(columnName.ToLower()) != -1)
						objectCount++;
					else if (objectCount == -1)
						continue;

					if (!_columnMap.ContainsKey(objectCount))
						_columnMap.Add(objectCount, new Dictionary<int, string>());

					_columnMap[objectCount].Add(i, columnName); ;
				}
				if (_columnMap.Count == 0) throw new Exception("The column map could not find any column identifiers, please check your SQL and 'SplitOn' parameter.");
			}
		}

		private class DoubleKeyDictionary<TKey1, TKey2, TValue> : Dictionary<TKey1, Dictionary<TKey2, TValue>> {

			public object Clone () {
				return new DoubleKeyDictionary<TKey1, TKey2, TValue>(this);
			}

			public TValue this[TKey1 k1, TKey2 k2] {
				get {
					return this.Get(k1, k2);
				}
				set {
					this.Add(k1, k2, value);
				}
			}

			public new IEnumerable<TValue> Values {
				get {
					foreach (KeyValuePair<TKey1, Dictionary<TKey2, TValue>> e1 in this) {
						foreach (KeyValuePair<TKey2, TValue> e2 in e1.Value) {
							yield return e2.Value;
						}
					}
				}
			}

			public DoubleKeyDictionary () { }

			public DoubleKeyDictionary (DoubleKeyDictionary<TKey1, TKey2, TValue> other) {
				foreach (KeyValuePair<TKey1, Dictionary<TKey2, TValue>> e1 in other) {
					foreach (KeyValuePair<TKey2, TValue> e2 in e1.Value) {
						this[e1.Key, e2.Key] = e2.Value;
					}
				}
			}

			public TValue Get (TKey1 k1, TKey2 k2) {
				if (this.ContainsKey(k1, k2)) {
					return base[k1][k2];
				} else {
					return default(TValue);
				}
			}

			public void Add (TKey1 k1, TKey2 k2, TValue value) {
				if (!base.ContainsKey(k1)) {
					base[k1] = new Dictionary<TKey2, TValue>();
				}
				base[k1][k2] = value;
			}

			public bool ContainsKey (TKey1 k1, TKey2 k2) {
				if (base.ContainsKey(k1)) {
					if (base[k1].ContainsKey(k2)) {
						return true;
					}
				}
				return false;
			}

			public void Remove (TKey1 k1, TKey2 k2) {
				if (this.ContainsKey(k1, k2)) {
					base[k1].Remove(k2);
					if (base[k1].Count == 0) {
						base.Remove(k1);
					}
				}
			}

			public ICollection<TValue> FindAll (Predicate<TValue> match) {
				List<TValue> results = new List<TValue>();
				foreach (TValue value in this.Values) {
					if (match(value)) {
						results.Add(value);
					}
				}
				return results;
			}
		}


		public static bool HasColumn (this IDataRecord dr, string columnName) {
			for (int i = 0; i < dr.FieldCount; i++) {
				if (dr.GetName(i).Equals(columnName, StringComparison.InvariantCultureIgnoreCase))
					return true;
			} return false;
		}

		public static object ReadValue (this IDataReader reader, int i, Type type) {
			try {
				switch (type.ToString()) {
					// Int16
					case "System.Int16":
						return reader.ReadInt16(i);
					case "System.Nullable`1[System.Int16]":
						return reader.ReadInt16OrNull(i);
					// UInt16
					case "System.UInt16":
						return reader.ReadUInt16(i);
					case "System.Nullable`1[System.UInt16]":
						return reader.ReadUInt16OrNull(i);
					// Int32
					case "System.Int32":
						return reader.ReadInt32(i);
					case "System.Nullable`1[System.Int32]":
						return reader.ReadInt32OrNull(i);
					// UInt32
					case "System.UInt32":
						return reader.ReadUInt32(i);
					case "System.Nullable`1[System.UInt32]":
						return reader.ReadUInt32OrNull(i);
					// Int64
					case "System.Int64":
						return reader.ReadInt64(i);
					case "System.Nullable`1[System.Int64]":
						return reader.ReadInt64OrNull(i);
					// UInt64
					case "System.UInt64":
						return reader.ReadUInt64(i);
					case "System.Nullable`1[System.UInt64]":
						return reader.ReadUInt64OrNull(i);
					// Double
					case "System.Double":
						return reader.ReadDouble(i);
					case "System.Nullable`1[System.Double]":
						return reader.ReadDoubleOrNull(i);
					// Single
					case "System.Single":
						return reader.ReadSingle(i);
					case "System.Nullable`1[System.Single]":
						return reader.ReadSingleOrNull(i);
					// Guid
					case "System.Guid":
						return reader.ReadGuid(i);
					case "System.Nullable`1[System.Guid]":
						return reader.ReadGuidOrNull(i);
					// Byte
					case "System.Byte":
						return reader.ReadByte(i);
					case "System.Nullable`1[System.Byte]":
						return reader.ReadByteOrNull(i);
					// SByte
					case "System.SByte":
						return reader.ReadSByte(i);
					case "System.Nullable`1[System.SByte]":
						return reader.ReadSByteOrNull(i);
					// DateTime
					case "System.DateTime":
						return reader.ReadDateTime(i);
					case "System.Nullable`1[System.DateTime]":
						return reader.ReadDateTimeOrNull(i);
					// DateTimeOffset
					case "System.DateTimeOffset":
						return reader.ReadDateTimeOffset(i);
					case "System.Nullable`1[System.DateTimeOffset]":
						return reader.ReadDateTimeOffsetOrNull(i);
					// Boolean
					case "System.Boolean":
						return reader.ReadBool(i);
					case "System.Nullable`1[System.Boolean]":
						return reader.ReadBoolOrNull(i);
					// Decimal
					case "System.Decimal":
						return reader.ReadDecimal(i);
					case "System.Nullable`1[System.Decimal]":
						return reader.ReadDecimalOrNull(i);
					// Byte[]
					case "System.Byte[]":
						return reader.ReadBinaryData(i);
					// String
					case "System.String":
						return reader.ReadString(i);
					// Char
					case "System.Char":
						return reader.ReadChar(i);
					case "System.Nullable`1[System.Char]":
						return reader.ReadCharOrNull(i);
				}

				// Enum
				if (type.IsEnum)
					return reader.ReadInt32(i);

				return null;

			} catch (InvalidCastException ex) {
				string columnName = reader.GetName(i);
				throw new InvalidCastException(String.Format(@"Invalid Cast: Expected field '{0}' to be of type '{1}'.", columnName, type.ToString()), ex);
			}
		}

		#region Read By Index

		public static Int16 ReadInt16 (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? (Int16)0 : reader.GetInt16(i);
		}

		public static Int16? ReadInt16OrNull (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? (Int16?)null : reader.GetInt16(i);
		}

		public static UInt16 ReadUInt16 (this IDataReader reader, int i) {
			return (UInt16)reader.ReadInt32(i);
		}

		public static UInt16? ReadUInt16OrNull (this IDataReader reader, int i) {
			return (UInt16?)reader.ReadInt32OrNull(i);
		}

		public static int ReadInt32 (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? 0 : reader.GetInt32(i);
		}

		public static int? ReadInt32OrNull (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? (int?)null : reader.GetInt32(i);
		}

		public static uint ReadUInt32 (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? 0 : (uint)reader.GetInt64(i);
		}

		public static uint? ReadUInt32OrNull (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? (uint?)null : (uint)reader.GetInt64(i);
		}

		public static Int64 ReadInt64 (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? 0 : reader.GetInt64(i);
		}

		public static Int64? ReadInt64OrNull (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? (Int64?)null : reader.GetInt64(i);
		}

		public static UInt64 ReadUInt64 (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? 0 : (UInt64)reader.GetDecimal(i);
		}

		public static UInt64? ReadUInt64OrNull (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? (UInt64?)null : (UInt64)reader.GetDecimal(i);
		}

		public static double ReadDouble (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? 0 : reader.GetDouble(i);
		}

		public static double? ReadDoubleOrNull (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? (double?)null : reader.GetDouble(i);
		}

		public static Single ReadSingle (this IDataReader reader, int i) {
			if (reader.GetDataTypeName(i) == "real")
				return reader.IsDBNull(i) ? 0 : (Single)reader.GetFloat(i);
			else
				return reader.IsDBNull(i) ? 0 : (Single)reader.GetDouble(i);
		}

		public static Single? ReadSingleOrNull (this IDataReader reader, int i) {
			if (reader.GetDataTypeName(i) == "real")
				return reader.IsDBNull(i) ? (Single?)null : (Single?)reader.GetFloat(i);
			else
				return reader.IsDBNull(i) ? (Single?)null : (Single?)reader.GetDouble(i);
		}

		public static Guid ReadGuid (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? Guid.Empty : reader.GetGuid(i);
		}

		public static Guid? ReadGuidOrNull (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? (Guid?)null : reader.GetGuid(i);
		}

		public static byte ReadByte (this IDataReader reader, int i) {
			if (reader.GetDataTypeName(i) == "tinyint")
				return reader.IsDBNull(i) ? (byte)0 : reader.GetByte(i);
			else
				return reader.ReadBinaryData(i)[0];
		}

		public static byte? ReadByteOrNull (this IDataReader reader, int i) {
			if (reader.GetDataTypeName(i) == "tinyint")
				return reader.IsDBNull(i) ? (byte?)null : reader.GetByte(i);
			else {
				byte[] bytes = reader.ReadBinaryData(i);
				if (bytes == null)
					return (byte?)null;

				return bytes[0];
			}
		}

		public static SByte ReadSByte (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? (SByte)0 : (SByte)reader.GetInt16(i);
		}

		public static SByte? ReadSByteOrNull (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? (SByte?)null : (SByte)reader.GetInt16(i);
		}

		public static DateTime ReadDateTime (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? DateTime.MinValue : reader.GetDateTime(i);
		}

		public static DateTime? ReadDateTimeOrNull (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? (DateTime?)null : reader.GetDateTime(i);
		}

		public static DateTimeOffset ReadDateTimeOffset (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? (DateTimeOffset)DateTime.MinValue : (DateTimeOffset)reader.GetValue(i);
		}

		public static DateTimeOffset? ReadDateTimeOffsetOrNull (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? (DateTimeOffset?)null : (DateTimeOffset)reader.GetValue(i);
		}

		public static bool ReadBool (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? false : reader.GetBoolean(i);
		}

		public static bool? ReadBoolOrNull (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? (bool?)null : reader.GetBoolean(i);
		}

		public static decimal ReadDecimal (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? 0 : reader.GetDecimal(i);
		}

		public static decimal? ReadDecimalOrNull (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? (decimal?)null : reader.GetDecimal(i);
		}

		public static Byte[] ReadBinaryData (this IDataReader reader, int i) {
			if (reader.IsDBNull(i)) {
				return null;
			} else {
				byte[] buffer = (byte[])reader.GetValue(i);
				return buffer;
			}
		}

		public static string ReadString (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? null : reader.GetString(i);
		}

		public static char ReadChar (this IDataReader reader, int i) {
			return Convert.ToChar(reader.GetString(i));
		}

		public static char? ReadCharOrNull (this IDataReader reader, int i) {
			return reader.IsDBNull(i) ? (char?)null : Convert.ToChar(reader.GetString(i));
		}

		public static int ReadInt (this IDataReader reader, int i) {
			return reader.ReadInt32(i);
		}

		public static int? ReadIntOrNull (this IDataReader reader, int i) {
			return reader.ReadInt32OrNull(i);
		}

		#endregion

		public static object ReadValue (this IDataReader reader, string name, Type type) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadValue(ordinal, type);
		}

		#region Read By Name

		public static Int16 ReadInt16 (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadInt16(ordinal);
		}

		public static Int16? ReadInt16OrNull (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadInt16OrNull(ordinal);
		}

		public static UInt16 ReadUInt16 (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadUInt16(ordinal);
		}

		public static UInt16? ReadUInt16OrNull (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadUInt16OrNull(ordinal);
		}

		public static int ReadInt32 (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadInt32(ordinal);
		}

		public static int? ReadInt32OrNull (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadInt32OrNull(ordinal);
		}

		public static uint ReadUInt32 (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadUInt32(ordinal);
		}

		public static uint? ReadUInt32OrNull (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadUInt32OrNull(ordinal);
		}

		public static Int64 ReadInt64 (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadInt64(ordinal);
		}

		public static Int64? ReadInt64OrNull (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadInt64OrNull(ordinal);
		}

		public static UInt64 ReadUInt64 (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadUInt64(ordinal);
		}

		public static UInt64? ReadUInt64OrNull (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadUInt64OrNull(ordinal);
		}

		public static double ReadDouble (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadDouble(ordinal);
		}

		public static double? ReadDoubleOrNull (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadDoubleOrNull(ordinal);
		}

		public static Single ReadSingle (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadSingle(ordinal);
		}

		public static Single? ReadSingleOrNull (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadSingleOrNull(ordinal);
		}

		public static Guid ReadGuid (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadGuid(ordinal);
		}

		public static Guid? ReadGuidOrNull (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadGuidOrNull(ordinal);
		}

		public static byte ReadByte (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadByte(ordinal);
		}

		public static byte? ReadByteOrNull (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadByteOrNull(ordinal);
		}

		public static SByte ReadSByte (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadSByte(ordinal);
		}

		public static SByte? ReadSByteOrNull (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadSByteOrNull(ordinal);
		}

		public static DateTime ReadDateTime (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadDateTime(ordinal);
		}

		public static DateTime? ReadDateTimeOrNull (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadDateTimeOrNull(ordinal);
		}

		public static DateTimeOffset ReadDateTimeOffset (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadDateTimeOffset(ordinal);
		}

		public static DateTimeOffset? ReadDateTimeOffsetOrNull (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadDateTimeOffsetOrNull(ordinal);
		}

		public static bool ReadBool (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadBool(ordinal);
		}

		public static bool? ReadBoolOrNull (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadBoolOrNull(ordinal);
		}

		public static decimal ReadDecimal (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadDecimal(ordinal);
		}

		public static decimal? ReadDecimalOrNull (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadDecimalOrNull(ordinal);
		}

		public static Byte[] ReadBinaryData (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadBinaryData(ordinal);
		}

		public static string ReadString (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadString(ordinal);
		}

		public static char ReadChar (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadChar(ordinal);
		}

		public static char? ReadCharOrNull (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadCharOrNull(ordinal);
		}

		public static int ReadInt (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadInt32(ordinal);
		}

		public static int? ReadIntOrNull (this IDataReader reader, string name) {
			int ordinal = reader.GetOrdinal(name);
			return reader.ReadInt32OrNull(ordinal);
		}

		#endregion
	}
}
