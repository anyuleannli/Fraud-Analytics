data=read.csv('data60.csv')
oot=read.csv("oot.csv")
library(tidyr)
library(dplyr)


data1=data %>% select(fraud_label,Days_since_fulladdress, fulladdress30, fulladdress14, 
         fulladdress7, fulladdress3, fulladdress1, Days_since_ssn, 
         ssn30, Days_since_firstname_ssn, Days_since_lastname_ssn, 
         Days_since_fulladdresshomephone, Days_since_nameDOB, ssnnameDOB30, 
         Days_since_ssnnameDOB, fulladdresshomephone30, nameDOB30,
         lastnamessn30, firstnamessn30, fulladdresshomephone14,
         nameDOB14, ssnnameDOB14, fulladdresshomephone7, nameDOB7, 
         ssn7, homephone3, homephone7,zip3_risk)

#fulladdress0,fulladdresshomephone3, homephone14, ssn3, homephone1, 
#lastnamessn3, homephone30, fulladdresshomephone1,ssn1, 
#ssnnameDOB1, nameDOB1, lastnamessn1, homephone0, fulladdresshomephone0, 
#ssn0, nameDOB0, fulladdressfirstname30, fulladdressnameDOB30, 
#fulladdresslastname30, Days_since_ssnhomephone)

oot1=oot%>%select(fraud_label,Days_since_fulladdress, fulladdress30, fulladdress14, 
                  fulladdress7, fulladdress3, fulladdress1, Days_since_ssn, 
                  ssn30, Days_since_firstname_ssn, Days_since_lastname_ssn, 
                  Days_since_fulladdresshomephone, Days_since_nameDOB, ssnnameDOB30, 
                  Days_since_ssnnameDOB, fulladdresshomephone30, nameDOB30,
                  lastnamessn30, firstnamessn30, fulladdresshomephone14,
                  nameDOB14, ssnnameDOB14, fulladdresshomephone7, nameDOB7, 
                  ssn7, homephone3, homephone7,zip3_risk)

smp_size <- floor(0.7 * nrow(data1))
train_ind <- sample(seq_len(nrow(data1)), size = smp_size)
test <- data1[-train_ind, ]
train=data1[train_ind, ]

##stepwise selection
library(olsrr)
model=lm(fraud_label~ ., data = train)
ols_step_both_p(model)


library(neuralnet)

################################
tic("NN Layer = 2")  ## 2212 sec
for (i in seq(1:10)) { 
  set.seed(i)
  trainid <- sample(1:nrow(data1), nrow(data1)*0.7 , replace=F) 
  train = train_test[trainid,]
  test = train_test[-trainid,]

  sum(train$fraud_label)
  sum(train$fraud_label)/nrow(train)
  sum(test$fraud_label)/nrow(test)
  
  NN2 = neuralnet(fraud_label ~ ., train, hidden = c(1,2) , act.fct = "logistic", linear.output = FALSE)
  
  predict_train = compute(NN2, train)
  predict_test = compute(NN2, test)
  predict_oot = compute(NN2, oot)
  
  train_NN = cbind(train, layer1 = predict_train$net.result)
  test_NN = cbind(test, layer1=predict_test$net.result)
  oot_NN = cbind(oot, layer1 = predict_oot$net.result)
  
  n_train = train_NN %>%
    arrange(desc(layer1)) %>%
    slice(1:round(0.03*nrow(train_NN))) %>%
    filter(fraud_label == 1) %>%
    nrow()
  
  n_test = test_NN %>%
    arrange(desc(layer1)) %>%
    slice(1:round(nrow(test_NN)*0.03)) %>%
    filter(fraud_label == 1) %>%
    nrow()
  
  n_oot = oot_NN %>%
    arrange(desc(layer1)) %>%
    slice(1:round(nrow(oot_NN)*0.03)) %>%
    filter(fraud_label == 1) %>%
    nrow()
  
  list_train2[[i]] = n_train/(nrow(train[train$fraud_label == 1,]))
  list_test2[[i]] = n_test/(nrow(test[test$fraud_label == 1,]))
  list_oot2[[i]] = n_oot/(nrow(oot[oot$fraud_label == 1,]))
  
}
toc()

predict_train = compute(NN2, train)
predict_test = compute(NN2, test)
predict_oot = compute(NN2, oot)

list_oot
list_train

plot(NN)



### Testing Code

trainid=sample(1:nrow(data), nrow(data1)*0.7 , replace=F) 
train = data1[trainid,]
test = data1[-trainid,]

NN2 = neuralnet(fraud_label ~ ., train, hidden = c(1,1) , act.fct = "logistic", linear.output = FALSE)

predict_train = compute(NN2, train)
predict_test = compute(NN2, test)


train_NN = cbind(train, layer1 = predict_train$net.result)
test_NN = cbind(test, layer1=predict_test$net.result)


n_train = train_NN %>%
  arrange(desc(layer1)) %>%
  slice(1:round(0.03*nrow(train_NN))) %>%
  filter(fraud_label == 1) %>%
  nrow()

n_test = test_NN %>%
  arrange(desc(layer1)) %>%
  slice(1:round(nrow(test_NN)*0.03)) %>%
  filter(fraud_label == 1) %>%
  nrow()


a = n_train/(nrow(train[train$fraud_label == 1,]))
b = n_test/(nrow(test[test$fraud_label == 1,]))


trainFDR=train%>%arrange(desc(layer1))
trainFDR=trainFDR[1:round(0.03*nrow(train))]
##check the dataset first
trainFDR$Fraud=ifelse(trainFDR$Fraud==1,1,0)
#compute FDR
sum(train_1FDR$Fraud)/sum(train$Fraud)

toc()

predict_train = compute(NN2, train)
predict_test = compute(NN2, test)

list_train

plot(NN2)


####################################################################
##restructure the dataset, 
sum(train$fraud_label) #currently we have 8375 of bad
#sample 83750 good from the the data, and bind it with our bad 
train2=train%>%
  filter(fraud_label==1) #train2 contains all the bad
train3=train%>%
  filter(fraud_label==0)
sum(train3$fraud_label)


good_ind <- sample(seq_len(nrow(train3)), size = 10*nrow(train2))
train5 <- train3[good_ind, ]
train6=rbind(train2,train5)
sum(train6$fraud_label)
#use train6 to replace train above for modeling



