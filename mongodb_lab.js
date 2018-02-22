
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


        
        db.movies.aggregate(
            [ { $sample: { size: 3 } } ]
         )

doc= db.movies.aggregate(
    [ { $sample: { size: 3 } } ]
 )
;
for (key in doc) print(key);




db.movies.aggregate([
    { $project : 
        { city_state : { $split: ["$city", ", "] }, qty : 1 } },
    { $unwind : "$city_state" },
    { $match : { city_state : /[A-Z]{2}/ } },
    { $group : { _id: { "state" : "$city_state" }, total_qty : { "$sum" : "$qty" } } },
    { $sort : { total_qty : -1 } }
  ]);




  db.movies.aggregate([
    { $project : 
        { _director : { $split: ["$director"," "] }, qty : 1 } 
    },
    {$match:{_director:{$in:["Emil"]}}
    }
  ]);


  use aggregations


  db.movies.aggregate([
    { $project : 
        { _awards : { $split: ["$awards"," "] }, qty : 1 } 
    },
    {$match:{_awards:{$in:["Won"]}}
    }
  ]);

  db.movies.aggregate([
    { $project : 
        { _awards : { $split: ["$awards"," "] }, qty : 1 } 
    },
    {$match:{$and:[
        {_awards:{$in:["Won"]}}
    ]}
    }
  ])



/*
  For all films that won at least 1 Oscar, 
  calculate the standard deviation, highest, 
  lowest, and average imdb.rating. 
  Use the sample standard deviation expression.
  */



 db.icecream_data.aggregate([
    { "$project": { "_id": 0, "max_high": { "$max": "$trends.avg_high_tmp" } } }
  ])
 
  -- {"imdb.rating":{"$gte":5,"$lt":5.5}}


  db.movies.aggregate([
    
    { "$project": { "_id": 0, "max_high": { "$max": "$imdb.rating" } } }
  ])


  db.movies.aggregate([
    { $project : 
        { _awards : { $split: ["$awards"," "] }, qty : 1 } 
    },
    {$match:{$and:[
        {_awards:{$in:["Won"]}}
    ]}
    },
    { $project:
             { "_id": 0, 
             "max_high": { "$max": "$imdb.rating" } } 
            }
    ])

  

  db.icecream_data.aggregate([
    { "$project": { "_id": 0, "min_low": { "$min": "$trends.avg_low_tmp" } } }
  ])



  db.movies.aggregate([
    { $project : 
        { city_state : { $split: ["$city", ", "] }, qty : 1 } },
    { $unwind : "$city_state" },
    { $match : { city_state : /[A-Z]{2}/ } },
    { $group : { _id: { "state" : "$city_state" }, total_qty : { "$sum" : "$qty" } } },
    { $sort : { total_qty : -1 } }
  ]);



  _awards

  db.movies.aggregate([
    { $project : 
        { $_awards : { $split: ["$awards", " "] }, qty : 1 } },
    { $unwind : "$_awards" },
    { $match : { "$_awards" :"Won" } },
    { $group : { _id:  "$_awards" , total_qty : { "$sum" : "$qty" } } },
    { $sort : { total_qty : -1 } }
  ])


  db.movies.aggregate([
    { $project : 
        { $_awards : { $split: [$awards, " "] }, qty : 1 } }
      ])


db.movies.aggregate([     
    { $project :          
        { _awards : { $split: ["$awards"," "] }, qty : 1 }      
    },
    { $unwind : "$_awards" },
    { $match : { "$_awards": /[A-Z]{2}/ } }
  ])


  db.movies.aggregate([     
    { $project :          
        { _awards : { $split: ["$awards"," "] }, qty : 1 }      
    },
    { $unwind : "$_awards" },
    { $match : { "$_awards": /[A-Z]{2}/ } }
  ])




  db.movies.aggregate([     
    { $project :          
        { _awards : { $split: ["$awards"," "] }, qty : 1 ,
        "imdb.rating":1
        }      
    },
    { $unwind : "$_awards" },
    { $match : { _awards: "Won" }
    },
    {$group:
    { _id:0, 
       max_rating : { "$max" : "$imdb.rating" },
       min_rating : { "$min" : "$imdb.rating" }
    }
    }
  ])



  --  { results: { $elemMatch: { $gte: 80, $lt: 85 } } }

  db.movies.aggregate([     
    { $project :          
        { _awards : { $split: ["$awards"," "] }, qty : 1 ,
        "imdb.rating":1
        }      
    },
    { $match : 
    { _awards:  /Oscar*/ } },
    {$group:
    { _id: "$_awards", 
       max_rating : { "$max" : "$imdb.rating" },
       min_rating : { "$min" : "$imdb.rating" },
       avg_rating : { "$avg" : "$imdb.rating" },
       std_rating : { "$stdDevPop" : "$imdb.rating" }
    }
    }
  ])

  db.movies.aggregate([     
    { $project :          
        { _awards : { $split: ["$awards"," "] }, qty : 1 ,
        "imdb.rating":1
        }      
    },
    { $match : 
    { _awards: /Won*Oscar*/ 
    },
    {$group:
    { _id: "$_awards", 
       max_rating : { "$max" : "$imdb.rating" },
       min_rating : { "$min" : "$imdb.rating" },
       avg_rating : { "$avg" : "$imdb.rating" },
       std_rating : { "$stdDevPop" : "$imdb.rating" }
    }
    }
  ])




  db.movies.aggregate([     
    { $project :          
        { _awards : "$awards",        
        "imdb.rating":1
        }      
    },
    { $match : 
    { _awards: /Won.[1-9].Oscar*/     }},
    {$group:
    { _id: 0, 
       max_rating : { "$max" : "$imdb.rating" },
       min_rating : { "$min" : "$imdb.rating" },
       avg_rating : { "$avg" : "$imdb.rating" },
       std_rating : "$round"( { "$stdDevPop" : "$imdb.rating" },4)
    }
    }
  ])


/*
 Let's use our increasing knowledge of the Aggregation Framework to explore our movies collection in more detail. 
 We'd like to calculate how many movies every cast member has been in and get an average imdb.rating for each cast member.

What is the name, number of movies, and average rating 
(truncated to one decimal) for the cast member that has been in the most number of movies with English as an available language?

Provide the input in the following order and format
{ "_id": "First Last", "numFilms": 1, "average": 1.1 }
{ "_id" : "John Wayne", "numFilms" : 107 , "average" : 6.4 }



*/


  db.movies.aggregate([
    /*for testing only*/
    {
      "$match": {
        "imdb.rating": { "$gt":7 },
        "year": { "$gte": 2010, "$lte": 2015 },
        "runtime": { "$gte": 90 },
        "languages":"English"
      }
    },
    {
      "$unwind": "$cast"
    },
    {
      "$group": {
        "_id": {
          "year": "$year",
          "cast": "$cast",
                 },
        "average_rating": { "$avg": "$imdb.rating" },
        "movies_count" :{"$sum":1}
      }},
   {
      "$sort": { "_id.cast": -1,}
    },
    {
      "$group": {
        "_id": "$_id.cast",
        "numFilms": { "$sum": "$_id.movies_count" },
        "average_rating": { "$first": "$average_rating" }
      }
    },
    {
      "$sort": { "_id": -1 }
    }
  ])

  db.movies.aggregate([
     {
      "$match": {
               "languages":"English"
      }
    },
    {
      "$unwind": "$cast"
    },
    {
      "$group": {
        "_id": {
               "cast": "$cast"
                 },
        "average_rating": { "$avg": "$imdb.rating" },
        "numFilms":{"$sum":1}

      }},
   {
      "$sort": { "_id.cast": -1}
    },
    {
      "$group": {
        "_id": "$_id.cast",
         "average_rating": { "$first": "$average_rating" },
         "total_numFilms":{"$sum":"$numFilms"}
      }
    },
    {
      "$sort": { "total_numFilms": -1 }
    }
  ])
/*
Which alliance from air_alliances flies the most routes with either a 
Boeing 747 or an Airbus A380 (abbreviated 747 and 380 in air_routes)?
*/


  db.air_alliances.aggregate([
    {"$lookup": {
        "from": "air_routes",
        "localField": "airlines",
        "foreignField": "airline.name",
        "as": "air_routes"
      }
    },
    {
        $match:
        {"air_routes.airplane":"747"}
    }
    ]).pretty()



    db.air_alliances.aggregate([
        {"$lookup": {
            "from": "air_routes",
            "localField": "airlines",
            "foreignField": "airline.name",
            "as": "air_routes"
          }
        },
        {
            $project:
            {"ariplanes":"$air_routes.airplane",
            "alliance":"$name"
            }
        },
        {$match:{
           "alliance":"SkyTeam" ,
           "ariplanes":{$in:["747"]}
        }},
        {$limit:10}
        ]).pretty()



        db.air_routes.aggregate([
            {$match:
                {airplane:{"$in":["747","380"]}}
            },
            {"$lookup": {
                "from": "air_alliances",
                "localField": "airline.name",
                "foreignField": "airlines",
                "as": "allinces"
              }
            },
            {$match:
            {"allinces.name":{$ne:""}}},
            {
                "$group": {
                  "_id": {
                         "allinces": "$allinces.name"
                           },
                           "rout_num":{"$sum":1}
                }},
                {$project:{"name":1,
                "rout_num":1
                            }},
                {
                    "$sort": { "rout_num": -1 }
                  }
            ]).pretty()

            
