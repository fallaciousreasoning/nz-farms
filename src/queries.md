# Select polygons touching 1

```sql
SELECT *
FROM "nz-property-titles-including-owners-1"
where not id=1 and MbrIntersects(Geometry, (
	select Geometry
	from "nz-property-titles-including-owners-1"
	where id=1
	))
```

# Select all titles that we touch

```sql
select *
from "nz-property-titles-including-owners-1" title
left join "nz-property-titles-including-owners-1" other_title
on not title.id = other_title.id and MbrIntersects(title.Geometry, other_title.Geometry)
LIMIT 5
```

# Select all close titles
```sql
select *
from "nz-property-titles-including-owners-1" title
left join "nz-property-titles-including-owners-1" other_title
on not title.id = other_title.id and Distance(title.Geometry, other_title.geometry) < 100
LIMIT 10000
```