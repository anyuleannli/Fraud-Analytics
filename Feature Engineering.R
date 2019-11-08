library(data.table)
ptm = proc.time()
app=fread("for_R.csv",stringsAsFactors = F)
library(dplyr)
library(lubridate)
##remove the first column
app=app%>%
  select(-V1)

app$date=ymd(app$date)
app$dob=ymd(app$dob)
app1=app

#Entitities for linking: ssn, fulladdress, namedob, phone
#Combination groups: (ssn,fulladdress), (ssn,namedob), (ssn, phone), 
#(fulladdress,namedob), (fulladdress,phone), (namedob,phone), (firstname,ssn), (lastname,ssn)…
#For each entity and combination group:
#Days since last seen: how many days since I last saw that entity or combination group
#Velocity: # records seen over the past n days, n = 0, 1, 3, 7, … whatever you like. How many times have I seen that entity or combination group over the past n days.

timeWinJoin = function(dt, n, byVar){
  dt1 = dt
  # Generate duplicated copy for the columns we'll join on
  # as they'll disappear after running the data.table join method,
  # n is the length of the time window
  dt1$join_ts1 = dt1$date
  dt1$join_ts2 = dt1$date + n
  dt1$join_rec = dt1$record
  # The join conditions below are equivalent to what in the sqldf code
  keys = c(byVar, 'join_ts1<=date', 'join_ts2>=date', 'record<=record')
  dt2 = dt1[dt, on=keys, allow.cartesian=T]
  return(dt2)
}

#combination group
for (i in c(0,1,3,7,14,30)){
  for (j in list(c('ssn','fulladdress'),c('ssn','nameDOB'),c('ssn','homephone'),c('fulladdress','nameDOB'),c('fulladdress','homephone'),c('nameDOB','homephone'),c('firstname','ssn'),c('lastname','ssn'))){
    assign(paste0("dt",j[1],j[2],i), timeWinJoin(app1, i, j))
  }
}

#for 0, 1, 3, 7  c('ssn','fulladdress'),c('ssn','nameDOB'),c('ssn','homephone'),c('fulladdress','nameDOB')
for (i in c(0,1,3,7)){
  for (j in list(c('ssn','fulladdress'),c('ssn','nameDOB'),c('ssn','homephone'),c('fulladdress','nameDOB'))){
    assign(paste0("dt",j[1],j[2],i), timeWinJoin(app1, i, j))
  }
}
#for 0, 1, 3, 7 c('fulladdress','homephone'),c('nameDOB','homephone'),c('firstname','ssn'),c('lastname','ssn')
for (i in c(0,1,3,7)){
  for (j in list(c('fulladdress','homephone'),c('nameDOB','homephone'),c('firstname','ssn'),c('lastname','ssn'))){
    assign(paste0("dt",j[1],j[2],i), timeWinJoin(app1, i, j))
  }
}

dtfirstnamessn0agg <- dtfirstnamessn0[, .(
  count = .N),
  by=record]

dtfirstnamessn1agg <- dtfirstnamessn1[, .(
  count = .N),
  by=record]

dtfirstnamessn3agg <- dtfirstnamessn3[, .(
  count = .N),
  by=record]

dtfirstnamessn7agg <- dtfirstnamessn7[, .(
  count = .N),
  by=record]


dtfulladdresshomephone0agg <- dtfulladdresshomephone0[, .(
  count = .N),
  by=record]

dtfulladdresshomephone1agg <- dtfulladdresshomephone1[, .(
  count = .N),
  by=record]

dtfulladdresshomephone3agg <- dtfulladdresshomephone3[, .(
  count = .N),
  by=record]

dtfulladdresshomephone7agg <- dtfulladdresshomephone7[, .(
  count = .N),
  by=record]


dtfulladdressnameDOB0agg <- dtfulladdressnameDOB0[, .(
  count = .N),
  by=record]

dtfulladdressnameDOB1agg <- dtfulladdressnameDOB1[, .(
  count = .N),
  by=record]

dtfulladdressnameDOB3agg <- dtfulladdressnameDOB3[, .(
  count = .N),
  by=record]

dtfulladdressnameDOB7agg <- dtfulladdressnameDOB7[, .(
  count = .N),
  by=record]


dtlastnamessn0agg <- dtlastnamessn0[, .(
  count = .N),
  by=record]

dtlastnamessn1agg <- dtlastnamessn1[, .(
  count = .N),
  by=record]

dtlastnamessn3agg <- dtlastnamessn3[, .(
  count = .N),
  by=record]

dtlastnamessn7agg <- dtlastnamessn7[, .(
  count = .N),
  by=record]

dtnameDOBhomephone0agg <- dtnameDOBhomephone0[, .(
  count = .N),
  by=record]

dtnameDOBhomephone1agg <- dtnameDOBhomephone1[, .(
  count = .N),
  by=record]

dtnameDOBhomephone3agg <- dtnameDOBhomephone3[, .(
  count = .N),
  by=record]

dtnameDOBhomephoneagg <- dtnameDOBhomephone7[, .(
  count = .N),
  by=record]

dtssnfulladdress0agg <- dtssnfulladdress0[, .(
  count = .N),
  by=record]

dtssnfulladdress1agg <- dtssnfulladdress1[, .(
  count = .N),
  by=record]

dtssnfulladdress3agg <- dtssnfulladdress3[, .(
  count = .N),
  by=record]

dtssnfulladdress7agg <- dtssnfulladdress7[, .(
  count = .N),
  by=record]

dtssnhomephone0agg <- dtssnhomephone0[, .(
  count = .N),
  by=record]

dtssnhomephone1agg <- dtssnhomephone1[, .(
  count = .N),
  by=record]

dtssnhomephone3agg <- dtssnhomephone3[, .(
  count = .N),
  by=record]

dtssnhomephone7agg <- dtssnhomephone7[, .(
  count = .N),
  by=record]

dtssnnameDOB0agg <- dtssnnameDOB0[, .(
  count = .N),
  by=record]

dtssnnameDOB1agg <- dtssnnameDOB1[, .(
  count = .N),
  by=record]

dtssnnameDOB3agg <- dtssnnameDOB3[, .(
  count = .N),
  by=record]

dtssnnameDOB7agg <- dtssnnameDOB7[, .(
  count = .N),
  by=record]
#drop unnecessary datatable
rm(dtfulladdresslastname0,dtfulladdresslastname1,dtfulladdresslastname3,dtfulladdresslastname7)

#for 0, 1, 3, 7 c('fulladdress','homephone'),c('nameDOB','homephone'),c('firstname','ssn'),c('lastname','ssn')
for (i in c(0,1,3,7)){
  for (j in list(c('fulladdress','firstname'),c('fulladdress','lastname'),c('firstname','homephone'),c('lastname','homephone'))){
    assign(paste0("dt",j[1],j[2],i), timeWinJoin(app1, i, j))
  }
}

dtfulladdresslastname0agg <- dtfulladdresslastname0[, .(
  count = .N),
  by=record]

dtfulladdresslastname1agg <- dtfulladdresslastname1[, .(
  count = .N),
  by=record]

dtfulladdresslastname3agg <- dtfulladdresslastname3[, .(
  count = .N),
  by=record]

dtfulladdresslastname7agg <- dtfulladdresslastname7[, .(
  count = .N),
  by=record]


#for 14, 30  c('ssn','fulladdress'),c('ssn','nameDOB'),c('ssn','homephone'),c('fulladdress','nameDOB')
for (i in c(14, 30)){
  for (j in list(c('ssn','fulladdress'),c('ssn','nameDOB'),c('ssn','homephone'),c('fulladdress','nameDOB'))){
    assign(paste0("dt",j[1],j[2],i), timeWinJoin(app1, i, j))
  }
}

#for 14, 30 c('fulladdress','homephone'),c('nameDOB','homephone'),c('firstname','ssn'),c('lastname','ssn')
for (i in c(14, 30)){
  for (j in list(c('fulladdress','homephone'),c('nameDOB','homephone'),c('firstname','ssn'),c('lastname','ssn'))){
    assign(paste0("dt",j[1],j[2],i), timeWinJoin(app1, i, j))
  }
}

for (i in c(14, 30)){
  for (j in list(c('nameDOB','homephone'))){
    assign(paste0("dt",j[1],j[2],i), timeWinJoin(app1, i, j))
  }
}

for (i in c(7)){
  for (j in list(c('nameDOB','homephone'))){
    assign(paste0("dt",j[1],j[2],i), timeWinJoin(app1, i, j))
  }
}

dtnameDOBhomephone7agg <- dtnameDOBhomephone7[, .(
  count = .N),
  by=record]

dtnameDOBhomephone14agg <- dtnameDOBhomephone14[, .(
  count = .N),
  by=record]

dtnameDOBhomephone30agg <- dtnameDOBhomephone30[, .(
  count = .N),
  by=record]
rm(dtnameDOBhomephone14,dtnameDOBhomephone30)

dtfulladdresshomephone14agg <- dtfulladdresshomephone14[, .(
  count = .N),
  by=record]

dtfulladdresshomephone30agg <- dtfulladdresshomephone30[, .(
  count = .N),
  by=record]

dtfulladdressnameDOB14agg <- dtfulladdressnameDOB14[, .(
  count = .N),
  by=record]

dtfulladdressnameDOB30agg <- dtfulladdressnameDOB30[, .(
  count = .N),
  by=record]


dtlastnamessn14agg <- dtlastnamessn14[, .(
  count = .N),
  by=record]

dtlastnamessn30agg <- dtlastnamessn30[, .(
  count = .N),
  by=record]


dtnameDOBhomephone14agg <- dtnameDOBhomephone14[, .(
  count = .N),
  by=record]

dtnameDOBhomephone30agg <- dtnameDOBhomephone30[, .(
  count = .N),
  by=record]


dtssnfulladdress14agg <- dtssnfulladdress14[, .(
  count = .N),
  by=record]

dtssnfulladdress30agg <- dtssnfulladdress30[, .(
  count = .N),
  by=record]

dtssnhomephone14agg <- dtssnhomephone14[, .(
  count = .N),
  by=record]

dtssnhomephone30agg <- dtssnhomephone30[, .(
  count = .N),
  by=record]

dtssnnameDOB14agg <- dtssnnameDOB14[, .(
  count = .N),
  by=record]

dtssnnameDOB30agg <- dtssnnameDOB30[, .(
  count = .N),
  by=record]

rm(dtfirstnamessn14,dtfirstnamessn30)


#for 14, 30 c('fulladdress','homephone'),c('nameDOB','homephone'),c('firstname','ssn'),c('lastname','ssn')
for (i in c(14, 30)){
  for (j in list(c('fulladdress','firstname'),c('fulladdress','lastname'),c('firstname','homephone'),c('lastname','homephone'))){
    assign(paste0("dt",j[1],j[2],i), timeWinJoin(app1, i, j))
  }
}

dtfirstnamehomephone14agg <- dtfirstnamehomephone14[, .(
  count = .N),
  by=record]

dtfirstnamehomephone30agg <- dtfirstnamehomephone30[, .(
  count = .N),
  by=record]

rm(dtfulladdresslastname14,dtfulladdresslastname30)

alldata=data.table(app1,dtfirstnamehomephone0agg[,2],dtfirstnamehomephone1agg[,2],dtfirstnamehomephone3agg[,2],dtfirstnamehomephone7agg[,2], dtfirstnamehomephone14agg[,2],dtfirstnamehomephone30agg[,2],
                   dtfirstnamessn0agg[,2],dtfirstnamessn1agg[,2],dtfirstnamessn3agg[,2],dtfirstnamessn7agg[,2],dtfirstnamessn14agg[,2],dtfirstnamessn30agg[,2],
                   dtfulladdresshomephone0agg[,2],dtfulladdresshomephone1agg[,2],dtfulladdresshomephone3agg[,2],dtfulladdresshomephone7agg[,2],dtfulladdresshomephone14agg[,2],dtfulladdresshomephone30agg[,2],
                   dtlastnamehomephone0agg[,2],dtlastnamehomephone1agg[,2],dtlastnamehomephone3agg[,2],dtlastnamehomephone7agg[,2],dtlastnamehomephone14agg[,2],dtlastnamehomephone30agg[,2],
                   dtlastnamessn0agg[,2],dtlastnamessn1agg[,2],dtlastnamessn3agg[,2],dtlastnamessn7agg[,2],dtlastnamessn30agg[,2],
                   dtssnhomephone0agg[,2],dtssnhomephone1agg[,2],dtssnhomephone3agg[,2],dtssnhomephone7agg[,2],dtssnhomephone14agg[,2],dtssnhomephone30agg[,2],
                   dtssnnameDOB0agg[,2],dtssnnameDOB1agg[,2],dtssnnameDOB3agg[,2],dtssnnameDOB7agg[,2],dtssnnameDOB14agg[,2],dtssnnameDOB30agg[,2],
                   dtfulladdressfirstname0agg[,2],dtfulladdressfirstname1agg[,2],dtfulladdressfirstname3agg[,2],dtfulladdressfirstname7agg[,2],dtfulladdressfirstname14agg[,2],dtfulladdressfirstname30agg[,2],
                   dtfulladdresslastname0agg[,2],dtfulladdresslastname1agg[,2],dtfulladdresslastname3agg[,2],dtfulladdresslastname7agg[,2],dtfulladdresslastname14agg[,2],dtfulladdresslastname30agg[,2],
                   dtnameDOBhomephone0agg[,2],dtnameDOBhomephone1agg[,2],dtnameDOBhomephone3agg[,2],dtnameDOBhomephone7agg[,2],dtnameDOBhomephone14agg[,2],dtnameDOBhomephone30agg[,2],
                   dtssnfulladdress0agg[,2],dtssnfulladdress1agg[,2],dtssnfulladdress3agg[,2],dtssnfulladdress7agg[,2],dtssnfulladdress14agg[,2],dtssnfulladdress30agg[,2],
                   dtfulladdressnameDOB0agg[,2],dtfulladdressnameDOB1agg[,2],dtfulladdressnameDOB3agg[,2],dtfulladdressnameDOB7agg[,2],dtfulladdressnameDOB14agg[,2],dtfulladdressnameDOB30agg[,2]
                   )


colnames(alldata)=c(colnames(alldata)[1:12],'firstnamehomephone0','firstnamehomephone1','firstnamehomephone3','firstnamehomephone7', 'firstnamehomephone14','firstnamehomephone30',
                   'firstnamessn0','firstnamessn1','dtfirstnamessn3','firstnamessn7','firstnamessn14','firstnamessn30',
                    'fulladdresshomephone0','fulladdresshomephone1','fulladdresshomephone3','fulladdresshomephone7','fulladdresshomephone14','fulladdresshomephone30',
                    'lastnamehomephone0','lastnamehomephone1','lastnamehomephone3','lastnamehomephone7','lastnamehomephone14','lastnamehomephone30',
                    'lastnamessn0','lastnamessn1','lastnamessn3','lastnamessn7','lastnamessn30',
                    'ssnhomephone0','ssnhomephone1','ssnhomephone3','ssnhomephone7','ssnhomephone14','ssnhomephone30',
                    'ssnnameDOB0','ssnnameDOB1','ssnnameDOB3','ssnnameDOB7','ssnnameDOB14','ssnnameDOB30',
                    'fulladdressfirstname0','fulladdressfirstname1','fulladdressfirstname3','fulladdressfirstname7','fulladdressfirstname14','fulladdressfirstname30',
                    'fulladdresslastname0','fulladdresslastname1','fulladdresslastname3','fulladdresslastname7','fulladdresslastname14','fulladdresslastname30',
                    'nameDOBhomephone0','nameDOBhomephone1','nameDOBhomephone3','nameDOBhomephone7','nameDOBhomephone14','nameDOBhomephone30',
                    'ssnfulladdress0','ssnfulladdress1','ssnfulladdress3','ssnfulladdress7','ssnfulladdress14','ssnfulladdress30',
                    'fulladdressnameDOB0','fulladdressnameDOB1','dtfulladdressnameDOB3','fulladdressnameDOB7','fulladdressnameDOB14','fulladdressnameDOB30')

write.csv(alldata,file='combination.csv')

days_since=fread("data_w_days_since.csv",stringsAsFactors = F)

days_since1=days_since[, -c(3:12)]
days_since1=days_since1[, -2]

alldata2=merge(x = alldata, y = days_since1, by = "record", all.x = TRUE)

entity=fread("alldata.csv",stringsAsFactors = F)
entity1=entity[, -c(3:12)]
entity1=entity1[, -2]

alldata3=merge(x = alldata2, y = entity1, by = "record", all.x = TRUE)

write.csv(alldata3,file='beforerisk.csv')



