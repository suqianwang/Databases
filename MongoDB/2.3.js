db.movies.aggregate([
	{
		$addFields: {
			year: { $substr: ["$release_date", 7, 4] }
		}
	},
	{
		$match: {
			year: {$ne: ""}
		}
	},
	{
		$group: {
			_id: { summary: "$summary", year: "$year" },
			count: { $sum: 1 }
		}
	},
	{
		$project: {
			_id: 0,
			count: 1,
			year: "$_id.year",
			summary: "$_id.summary"
		}
	},
	{
		$sort: {
			year: 1,
			summary: -1
		}
	},
	{
		$limit: 100
	}
]).toArray()