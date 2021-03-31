db.movies.aggregate([
	{
		$match: {
			"genre.Adventure": true
		}
	},
	{
		$group: {
			_id:{ movie_title: "$movie_title" },
			count: { $sum: 1 },
			average_rating: { $avg: "$rating" }
		}
	},
	{
		$match: {
			count: { $gte: 100 }
		}
	},
	{
		$project: {
			_id: 0,
			average_rating: "$average_rating",
			movie_title: "$_id.movie_title"
		}
	},
	{
		$sort: {
			average_rating: -1
		}
	},
	{
		$limit: 20
	}
]).toArray()