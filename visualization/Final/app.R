library(shiny)
library(shinydashboard)
library(leaflet)
library(dplyr)
library(RColorBrewer)
library(leaflet.extras)
library("KernSmooth")
library("data.table")
library("sp")
library("rgdal")
library(tm)
library(wordcloud)
library(wordcloud2)
library(stopwords)
library(memoise)
library(raster)
library(ggplot2)
library("plotly")

#source('dataGeneration.R')

load(".RData")

#Dataframe of all languages and their associated ISO codes ----------------
languages = data.frame(
  name = c("Spanish",
           "English",
           "Portuguese", 
           "Danish",
           "French",
           "Italian",
           "German",
           "Turkish",
           "Dutch",
           "Hungarian",
           "Finnish",
           "Russian",
           "Swedish"),
  code = c("es", 
           "en", 
           "pt",
           "da", 
           "fr",
           "it",
           "de",
           "tr",
           "nl", 
           "hu", 
           "fi", 
           "ru", 
           "sv"))

#Plot of tweet time series ---------------------
ggplot(data = time.series, aes(x = Time, y = Tweets, group = 1))+
  geom_line(color = "#00AFBB", size = 0.1)+
  labs(x = "Date", y = "Number of tweets", title = "Number of tweets posted at a given date")

#Method to perfom word frequency counting for generating the word cloud -----------------
getTermMatrix <- memoise(function(langName, date) {
  
  lang = languages %>% filter(name == langName)
  
  textAll = unlist(data %>%
                     filter(Tweet.language..ISO.639.1. == lang$code) %>% 
                     filter(Date == date) %>%
                     dplyr::select(HashTags), recursive = FALSE)
  
  text = (textAll[lapply(textAll, length)>0])
  
  docs = Corpus(VectorSource(text))
  
  toSpace <- content_transformer(function (x , pattern ) gsub(pattern, " ", x))
  docs <- tm_map(docs, toSpace, "/")
  docs <- tm_map(docs, toSpace, "@")
  docs <- tm_map(docs, toSpace, "\\|")
  
  # Convert the text to lower case
  docs <- tm_map(docs, content_transformer(tolower))
  # Remove numbers
  docs <- tm_map(docs, removeNumbers)
  # Remove language common stopwords
  docs <- tm_map(docs, removeWords,stopwords::stopwords(tolower(lang$name), source = "stopwords-iso"))
  # Remove punctuations
  docs <- tm_map(docs, removePunctuation)
  # Eliminate extra white spaces
  docs <- tm_map(docs, stripWhitespace)
  # Remove your own stop word
  # specify your stopwords as a character vector
  docs <- tm_map(docs, removeWords, c("https", "tco", "...", "com"))
  
  dtm <- TermDocumentMatrix(docs)
  m <- as.matrix(dtm)
  v <- sort(rowSums(m),decreasing=TRUE)
  d <- data.frame(word = names(v),freq=v)
})

#INTERFACE ------------------------------------------
ui <- navbarPage(
  "Twitter Data Explore",
  #HEAT MAP ===========================================
  tabPanel("Where?",
           titlePanel("Explore where people are tweeting from"),
           br(),
           sidebarLayout(
             sidebarPanel(
               textInput("wordFilter", "Filter by word: ", value = ""),
               selectInput("mapLang", "Choose a language:",
                           choices = languages$name),
               sliderInput("opacity", label = "Opacity",
                           min = 0, max = 1,
                           value = 0.8),
               sliderInput("gridSize", label = "Grid Size",
                           min = 1, max = 2000,
                           value = 100),
               sliderInput("bandwidth", label = "Bandwidth",
                           min = 0.000001, max = 0.5,
                           value = 0.001),
               sliderInput("mapDateSlider", label = h3("Date"),
                           min = min(data$Date), max = max(data$Date),
                           value = c(min(data$Date),max(data$Date))),
             ),
             mainPanel(
               leafletOutput("map") 
             )
           )
  ),
  #Time Series ===========================================
  tabPanel("When?",
           titlePanel("Explore when people are tweeting"),
           br(),
           sidebarLayout(
             sidebarPanel(
               helpText("Select a time interval to examine."),
               sliderInput("seriesDateSlider", label = h3("Date"),
                           min = min(time.series$Date), max = max(time.series$Date),
                           value = c(min(time.series$Date),max(time.series$Date)))
             ),
             mainPanel(
               plotlyOutput("series")
             )
             
           )
  ),
  #Word Cloud ===========================================
  tabPanel("What?",
           titlePanel("Explore what people are tweeting about"),
           br(),
           sidebarPanel(
             selectInput("cloudLang", "Choose a language:",
                         choices = languages$name),
             selectInput("cloudDate", "Choose a date:",
                         choices = c("2016-04-14", "2016-04-15","2016-04-16","2016-04-17", "2016-04-18","2016-04-19", 
                                     "2016-04-20","2016-04-21","2016-04-22")),
             sliderInput("cloudNumberHashTagsSlider", label = h3("Number of HashTags:"),
                         min = 10, max = 100,
                         value =25),
             #actionButton("update", "Change")
           ),
           
           mainPanel(
             wordcloud2Output("cloud", width = "100%", height = "400px"),
             br(),
             br(),
             plotlyOutput("barplot")
           )
  )
)






#SERVER ------------------------------------------
server <- function(input, output) {
  # HEATMAP  ===========================================
  output$map <- renderLeaflet({
    lang = languages %>% filter(name == input$mapLang)
    
    dat <- data %>% 
      filter(Latitude < 44) %>% 
      filter(Latitude > 35) %>% 
      filter(Longitude > -12) %>% 
      filter(Longitude < 7) %>% 
      filter(Tweet.language..ISO.639.1. == lang$code) %>% 
      filter(Date >= input$mapDateSlider[1]) %>%
      filter(Date <= input$mapDateSlider[2]) %>% 
      filter(grepl(tolower(input$wordFilter),tolower(Tweet.content)))
    
    dat <- data.table(dat)
    kde <- bkde2D(dat[ , list(Longitude, Latitude)],
                  bandwidth=c(input$bandwidth, input$bandwidth), 
                  gridsize = c(input$gridSize,input$gridSize))
    
    KernelDensityRaster <- raster(list(x=kde$x1 ,y=kde$x2 ,z = kde$fhat))
    KernelDensityRaster@data@values[which(KernelDensityRaster@data@values < 0.001)] <- NA
    
    palRaster <- colorNumeric(viridis::viridis(n = 500), domain = KernelDensityRaster@data@values, na.color = "transparent")
    
    leaflet() %>% addTiles() %>% 
      addProviderTiles(providers$CartoDB.DarkMatter) %>% 
      addRasterImage(KernelDensityRaster, 
                     colors = palRaster, 
                     opacity = input$opacity) %>%
      addLegend(pal = palRaster, 
                values = KernelDensityRaster@data@values, 
                title = "Kernel Density of Points") %>% 
      setView(lng = -3.7, lat = 40.4, zoom = 06) %>% 
      setMaxBounds( lng1 = -13
                    , lat1 = 35
                    , lng2 = 7
                    , lat2 = 44 )
  })
  
  
  # TIME SERIES  ========================================
  get.data <- reactive({
    min<-as.Date(input$seriesDateSlider[1])
    max<-as.Date(input$seriesDateSlider[2])
    
    time.series<-time.series%>%filter(Date>=min & Date<=max)
    
    times<-(time.series$Time)
    time.format<- "%Y-%m-%d %H:%M"
    times.formatted <-(as.POSIXct(times, format=time.format)) 
    time.series$Time<- times.formatted
    return(as.data.frame(time.series))
  })
  
  output$series <- renderPlotly({
    ggplot(data = get.data(), aes(x = Time, y = Tweets, group = 1))+
      geom_line(color = "#00AFBB", size = 0.1)+
      labs(x = "Time", y = "Number of tweets", title = "Number of tweets posted at a given time")
  })
  
  # WORDCLOUD  ===========================================
  terms <- reactive({
    # Change when the "update" button is pressed...
    #input$update
    # ...but not for anything else
    #isolate({
      withProgress({
        setProgress(message = "Processing tweets...")
        words<-getTermMatrix(input$cloudLang, input$cloudDate)
        return(words)
      })
  })
  
  
  output$cloud <- renderWordcloud2({
    data=terms()
    amount<-input$cloudNumberHashTagsSlider[1]
    wordcloud2(data[1:amount,],
               size = 0.7, shape = 'pentagon',
               color = "#00AFBB")
  })
  
  output$barplot <- renderPlotly({
    data=terms()
    data<-data[1:10,]
    # Factor levels in decreasing order
    data$word <- factor(data$word,levels = data$word[order(data$freq, decreasing = TRUE)])
    
    ggplot(data, aes(x=word, y=freq)) +
      geom_bar(stat="identity", fill = "#00AFBB") +
      theme(axis.text.x = element_text(angle = 90, hjust = 1))+
      labs(x = "", y = "Number of uses", title = "Top 10 HashTags")
  })
}

#RUN APP ------------------------------------------
shinyApp(ui = ui, server = server)