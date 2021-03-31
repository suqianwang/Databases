db.movies.aggregate([
	{
		$match: {
			age: {$lt: 15},
			"genre.Horror": true
		}
	},
	{
		$group: {
			_id:{ user_id: "$user_id"},
			count: { $sum: 1 }
		}
	},
	{
		$match: {
			count: { $gte: 2}
		}
	},
	{
		$project: {
			_id:0, 
			user_id: "$_id.user_id"
		}
	},
	{
		$sort: {
			user_id: 1
		}
	}
]).toArray()