
# @author: Dr Adam Varley, University of Stirling, Created using funding provided by NERC under the ASSIST project subsidery of the Unconventional Hydrocarbons project 
# Created on 29-03-2021
# Unlicensed but would be grateful for a citation if used and it is important that you apropriately cite Twitter for their wonderful contribution to science in V2!!
# Also the code is heavily reliant on the below dependencies

library(httr)
library(jsonify)
library(sf)
library(raster)
library(tidyverse)
library(RPostgreSQL)
library(lubridate)
library(geojsonsf)
library(mongolite)
library(jsonlite)


# I have chosen to use Mongo as the json results that come out of the API are unstructured and flattening proved challenging. It is not ideal, but is preferable to saving all data into an
#unmanageble JSON file. Feel free to flatten results, although owed to the nature and complexity of the output this might be challenging and needs some real thought. 

# The connection to the mongo DB
conn =  "your-mongo connection"

bearer_token <- "your-token"
headers <- c(`Authorization` = sprintf('Bearer %s', bearer_token))

i=2
# Function for pulling correct datetime from Twitter response
send_Mongo <- function(data,con){
  if(!is.null(data)){
    # for list
    for(i in 1:length(data)){
      # Try and force ISO timestamp to Posixct
      tryCatch(expr = {if(!is.null(data[[i]]$created_at)){
        data[[i]]$created_at <- as.POSIXct(data[[i]]$created_at,format="%Y-%m-%dT%H:%M:%OS")
      }}, error = function (err) {
      }
      )
      
      #Add ingestion date
      data[[i]]$ingestion_date <- Sys.time()
      item <- data[[i]]
      con$insert(item)
      returned <- 'Submitted'
    }
  } else {
    returned <- 'No data to submit'
  }
  return(returned)
}

# Main function
send_token_retrieve_data <- function(query_term_combined,tweets_per_request,start_date_request,end_date_request, next_token,headers){
  
  # Return everything, see API for details
  params = list(
    query = query_term_combined,
    max_results = tweets_per_request,
    start_time= start_date_request,
    end_time= end_date_request,
    tweet.fields = 'attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,reply_settings,source,text,withheld',
    expansions = 'attachments.poll_ids,attachments.media_keys,author_id,entities.mentions.username,geo.place_id,in_reply_to_user_id,referenced_tweets.id,referenced_tweets.id.author_id',
    user.fields = 'id,username,location,created_at,description',
    place.fields = 'contained_within,country,country_code,full_name,geo,id,name,place_type',
    media.fields = 'duration_ms,height,media_key,preview_image_url,type,url,width,public_metrics'
  )
  # Next token will allow for pagination, kill if no token returned
  if(next_token != 0){params = c(params,next_token = next_token)}
  # message('querying Twitter')
  
  # Search the correct endpoint - must have authentification for this - please see Twitter developer guide 
  response <- httr::GET(url = 'https://api.twitter.com/2/tweets/search/all', httr::add_headers(.headers=headers), query = params)
  
  obj <- httr::content(response, as = "text")
  tryCatch({
    list_tweets <- jsonify::from_json(obj,simplify = FALSE)

  
  
  # Pull next token
  next_token <- list_tweets$meta$next_token
  tweet_tot <- ifelse(is.null(list_tweets$meta$result_count),0, list_tweets$meta$result_count)
  
  # insert into MongoDB tables the 4 separate tree responses from Twitter
  DBtweets  <- mongo(collection = 'tweets',db = db_name,url = conn)
  DBusers  <- mongo(collection = 'users',db = db_name,url = conn)
  DBplaces <- mongo(collection = 'places',db = db_name,url = conn)
  DBtweetsInfo <- mongo(collection = 'tweets_info',db = db_name,url = conn)
  
  tryCatch(expr = {
    send_Mongo(data = list_tweets$data,con = DBtweets)
  },error = function(err) {
    message(err)
  })
  
  tryCatch(expr = {
    send_Mongo(data = list_tweets$includes$users,con = DBusers)
  },error = function(err) {
    message(err)
  })
  
  tryCatch(expr = {
      send_Mongo(data = list_tweets$includes$places,con = DBplaces)
      }}
  ,error = function(err) {
    message(err)
  }
  )
  
  tryCatch(expr = {
    send_Mongo(list_tweets$includes$tweets,con = DBtweetsInfo)
  },error = function(err) {
    message(err)
  })
  
  if(!is.null(list_tweets$data)){
    Sys.sleep(3)
  } else {
    Sys.sleep(2)
  }
  
  return_frame <- list(next_token = next_token, tweets_total = tweet_tot)
  return(return_frame)
  },error = function(err) { 
    message(obj)
    if(grepl('limit',obj)){
      message('Script sleeping for 30 seconds as request limit has been reached')
      Sys.sleep(30)
    }
  })
}

# Wrapper round main function to call per day
# I tried to pull much larger queries returning 100,000 of records over years and found the tokens droppped out. Therefore, stuck to days and looped
keyword_spatial_search <- function(db_name,start_date,end_date,spatial_query,keyword_query,bbox,tweets_per_request){
  dropouts <- character()
  start_date <- as.POSIXct(start_date,format="%Y-%m-%dT%H:%M:%OS")
  end_date <- as.POSIXct(end_date,format="%Y-%m-%dT%H:%M:%OS")
  date_sequence <- seq(start_date,end_date,by = 'year')
  
  query_term_combined = paste(keyword_query,spatial_query)
  time_frame <- data.frame(from = as.character(date_sequence[-length(date_sequence[-1])],format="%Y-%m-%dT%H:%M:%OSZ"),to = as.character(date_sequence[-1],format="%Y-%m-%dT%H:%M:%OSZ"))
  i = 7
  tweets_per_run <- 0
  for(i in nrow(time_frame):1){
    message('collecting tweets for : ', time_frame$from[i])
    tweets_tot <- 0
    next_token <- 0
    while(!is.null(next_token)){
      tryCatch(expr = {
        list_data <- send_token_retrieve_data(query_term_combined,
                                              tweets_per_request,start_date_request = time_frame$from[i],end_date_request = time_frame$to[i], 
                                              next_token,headers)
        next_token <- list_data$next_token
        tweets_tot <- tweets_tot + list_data$tweets_tot
        
        }
        ,error = function(err) {
          message(time_frame$from[i] ,' dropped out \n')
          message(err)
          
          dropouts <- c(as.character(time_frame$from[i]),dropouts)
        })
      
      tweets_per_run <- tweets_per_run + tweets_tot
      
    }
    message('collected : ', tweets_tot, ' tweets')
  }
  
  return(list(count = tweets_per_run, dropouts = dropouts))
}

##################################################
################ Spatial data ####################
##################################################

# Get UK outlines
UK <- getData('GADM', country = 'GB', level = 0) %>% st_as_sf()

# needs to be less than 25 miles for API, so 0.2 is roughly 22 miles in degrees
grid_size = 0.2
search_grid <- UK %>% 
  st_make_grid(cellsize = 0.2, what = "polygons") %>% st_intersection(UK) %>% st_as_sf()

# Function to pull bounding box for each feature
st_bbox_by_feature = function(x) {
  x = st_geometry(x)
  f <- function(y) st_as_sfc(st_bbox(y))
  do.call("c", lapply(x, f))
}

# Turn into sf
search_grid <- st_bbox_by_feature(search_grid) %>% st_as_sf()

# Plot coordinates to check
plot(search_grid,col = 'grey')

keyword_query = '(tremor OR quake OR shaking OR shook OR wobble OR seism OR quiver OR tremble OR shudder OR sway OR rock OR shudder OR "earth move" OR tectonic) lang:en'

plot_progress <- search_grid
plot_progress$counts <- NA
i=30
for(i in 1:nrow(search_grid)){
  message('##################################\n   Reading data for block - ', i,'\n##################################\n' )
  ext <- extent(search_grid[i,'x'])
  
  # format bounding box 
  spatial_query <- paste0('bounding_box:[',paste(ext[1],ext[3],ext[2],ext[4],collapse = ' '),']')
  
  # call main function and give a new Mongo output DB. 
  data_returned <- keyword_spatial_search(db_name = 'earthquakes',start_date = '2006-04-01T00:00:00Z',end_date = '2021-05-09T00:00:00Z', spatial_query = spatial_query , keyword_query = keyword_query,tweets_per_request = 100)
  block_count <- ifelse(is.null(data_returned$count),0,data_returned$count)
  plot_progress$counts[i] <- block_count
  plot(plot_progress['counts'])
}



