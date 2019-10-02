
library(parallel)
library(doParallel)

position = read.csv("./data/Position.csv",stringsAsFactors = FALSE)
userid = unique(position$user_id)

## Parallel processing
## separated by user_id and Sorted by position_id
user_sep = mclapply(userid, function(x){
                  df = position[position$user_id == x,]
                  df[order(df$user_id),]
                  },mc.cores = detectCores())


#manhattan distance
manhattan = function(x, y){
  ## x, y are two vectors of coordinates
  invisible(abs(y[1] - x[1]) + abs(y[2] - x[2]))  
}

user_distance = function(userdata, id = 'user_id', coord = 'coordinate'){
  id = unique(as.vector(t(userdata[id])))
  #print(id)
  distance = 0
  coordinates = as.vector(t(userdata[coord]))
  for (i in 1:length(coordinates)){
    if(i==1){x=c(0,0)}else{x = as.numeric(unlist(strsplit(coordinates[i-1],split = ',')))}
    y = as.numeric(unlist(strsplit(coordinates[i],split = ',')))
    distance = sum(distance, manhattan(x,y))
    print(distance)
    
  }
  invisible(c(id, distance))
}


submission = do.call(rbind,mclapply(user_sep, user_distance,id = 'user_id', coord = 'coordinate', 
         mc.cores = detectCores()))
submission = data.frame(submission)
colnames(submission) = c('user_id', 'distance')

write.csv(submission, file = 'submission.csv', row.names = FALSE)

