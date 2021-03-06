db.movies.aggregate([
	{
		$group: {
			_id: { summary: "$summary" },
			count: { $sum: 1 }
		}
	},
	{
		$project: {
			_id: 0,
			summary: "$_id.summary",
			count: 1
		}
	},
	{
		$sort: {
			count: 1
		}
	}
]).toArray()