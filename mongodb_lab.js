
 mongo "mongodb://cluster0-shard-00-00-jxeqq.mongodb.net:27017,cluster0-shard-00-01-jxeqq.mongodb.net:27017,cluster0-shard-00-02-jxeqq.mongodb.net:27017/aggregations?replicaSet=Cluster0-shard-0" --authenticationDatabase admin --ssl -u m121 -p aggregations --norc






var favorites = [
    "Sandra Bullock",
    "Tom Hanks",
    "Julia Roberts",
    "Kevin Spacey",
    "George Clooney"]

    var favorites = [
        "Sandra Bullock"]


db.movies.aggregate([

   {"$addFields": {"v1": "$tomatoes.viewer.rating" }},
   {"$project":{
    "_id":0,
     "v1":1,
     "countries":1,
     "cast":1
     }
},
   {
    $match:{
        "countries":{"$in":["USA"]},
        "v1":{"$gt":3},
        "cast":{"$in":["Sandra Bullock" , "Tom Hanks",
                        "Julia Roberts",
                        "Kevin Spacey",
                        "George Clooney","Denzel Washington"]}
    }
}
])


-- Unwind--


db.BusinessProcess.aggregate( 
    {"$group" :
    {_id : {status:"$status", type: "$type"}, 
    count : { $sum : 1} 
    } 
  } )

db.movies.aggregate([
    {"$unwind":"$cast"},
    {"$addFields": 
        {"v1": "$tomatoes.viewer.rating" }
    },
    {"$project":{
     "_id":0,
      "v1":1,
      "countries":1,
      "cast":1,
      "title":1
      }
 },
    {
     $match:{
         "countries":{"$in":["USA"]},
         "v1":{"$gte":3},
         "cast":{"$in":["Sandra Bullock" , "Tom Hanks",
                         "Julia Roberts",
                         "Kevin Spacey",
                         "George Clooney"]}
     }},
     {"$group" :
      {_id : { title: "$title"}, 
      num_favs : { $sum : 1} 
      } },
     {"$sort":{"num_favs":-1, "v1":-1,"title":-1}}
 ])
 
-- 'QA'
{ "_id" : { "title" : "While You Were Sleeping", "rank" : 3.2 }, "count" : 1 }


db.movies.aggregate([
    {$match:{
        "title":{$eq:"Out of Sight"}}
    }])

    db.movies.aggregate([
        {$match:{
            "title":{$eq:"Recount"}}
        }])

        


    db.movies.aggregate([
        {"$unwind":"$cast"},
        {"$addFields": 
            {"v1": "$tomatoes.viewer.rating" }
        },
        {"$project":{
         "_id":0,
          "v1":1,
          "countries":1,
          "cast":1,
          "title":1
          }
     },
        {
         $match:{
             "countries":{"$in":["USA"]},
             "v1":{"$gte":3},
             "cast":{"$in":["Sandra Bullock" , "Tom Hanks",
                             "Julia Roberts",
                             "Kevin Spacey",
                             "George Clooney"]}
         }}
     ])




     {$skip: 24}, {$limit: 1}


     db.BusinessProcess.aggregate( 
        {"$group" :
        {_id : {status:"$status", type: "$type"}, 
        count : { $sum : 1} 
        } 
      } )
     --- > Sort your results by num_favs, 
     --tomatoes.viewer.rating, and title, all in descending order.

    db.movies.aggregate([
        {"$unwind":"$cast"},
        {"$addFields": 
            {"rating": "$tomatoes.viewer.rating" }
        },
        {
         $match:{
             "countries":{"$in":["USA"]},
             "rating":{"$gte":3},
             "cast":{"$in":["Sandra Bullock" , "Tom Hanks",
                             "Julia Roberts",
                             "Kevin Spacey",
                             "George Clooney"]}
         }},
         {"$group" :
          {_id : { title: "$title", v1:"$v1"}, 
          num_favs : { $sum : 1} ,
          } },
         {$sort:{"num_favs":-1, "rating":-1,"title":-1}},
         {$skip:24},
         {$limit:1}
             ])
'----------------------------------'
/* 
Calculate an average rating for each movie in our collection 
where English is an available language, 
the minimum imdb.rating is at least 1, the minimum imdb.votes is at least 1, 
and it was released in 1990 or after. 
You'll be required to rescale (or normalize) imdb.votes. 
The formula to rescale imdb.votes and calculate normalized_rating is included as a handout.

"languages" : [		"English" 	],
 
*/

db.movies.aggregate([
        {$match:{
        "title": "DMZ"}
    },
    { $project: {
        "year":1,
        "title":1}}
    ])

    Calculate an average rating for each movie in our collection 
    where English is an available language, the minimum imdb.rating is at least 1, 
    the minimum imdb.votes is at least 1, and it was released in 1990 or after. 
    You'll be required to rescale (or normalize) imdb.votes. The formula to rescale 
    imdb.votes and calculate normalized_rating is included as a handout.

   /*  {
        $project: {
          quizAvg: { $avg: "$quizzes"},
          labAvg: { $avg: "$labs" },
          examAvg: { $avg: [ "$final", "$midterm" ] }
        }
      } */

/*
given we have the numbers, this is how to calculated normalized_rating
yes, you can use $avg in $project and $addFields!
normalized_rating = average(scaled_votes, imdb.rating)
*/


    db.movies.aggregate([
        {"$addFields": 
            {"x_max" : 1521105,
             "x_min": 5,
             "min": 1,
             "max": 10,
             "votes": "$imdb.votes",
             "normelized":
                        {
                            $add: [
                            "$min",
                            {
                                $multiply: [
                                    {$subtract: ["$max", "$min"]},
                                {
                                    $divide: [
                                    { $subtract: ["$x", "$x_min"] },
                                    { $subtract: ["$x_max", "$x_min"] }
                                    ]
                                }
                                ]
                            }
                            ]
                        }
             }
        },
        {$match:{
            "languages": {"$in":["English"]},
            "imdb.rating": {$gt:1},
            "imdb.votes":{$gt:1},
            "year":{$gte:1990}
        }},
        { $project: {
            "normelized":1,
            "normalized_rating":{$avg:["$normelized", "$imdb.rating"]},
            "year":1,
            "votes":1,
            "title":1
            }},
          {$sort:{"normalized_rating":1}
        },
        {$limit:10}
        ]).pretty()

