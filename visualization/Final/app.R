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


data <- read.csv("../data/clean-twitter-data.csv", fileEncoding = "latin1")
data$Date <- as.Date(data$Date)
data$HashTags = regmatches(data$Tweet.content, gregexpr("#(\\d|\\w)+", data$Tweet.content))



#Using "memoise" to automatically cache the results
getTermMatrix <- memoise(function(langName, startDate, endDate) {
  
  lang = languages %>% filter(name == langName)
  
  
  textAll = unlist(data %>%
                     filter(Tweet.language..ISO.639.1. == lang$code) %>% 
                     filter(Date == startDate) %>%
                     #filter(Date >= startDate) %>%
                     #filter(Date <= endDate) %>%
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



ui <- dashboardPage(
  
  dashboardHeader(),
 
   # SIDE BAR
  dashboardSidebar(
    textInput("wordFilter", "Filter by word: ", value = ""),
    selectInput("lang", "Choose a language:",
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
    sliderInput("dateSlider", label = h3("Date"),
                min = min(data$Date), max = max(data$Date),
                value = c(min(data$Date),max(data$Date))),
    actionButton("update", "Change")
  ),
  
  
  # BODY, BACKGROUND
  dashboardBody(
    tags$style(type = "text/css", "#map {height: calc(100vh - 80px) !important;}"),
    leafletOutput("map"),
    
    wordcloud2Output("cloud")
    
    # absolutePanel(id="controls",
    #               style="z-index:500;",
    #               class = "panel panel-default",
    #               draggable = TRUE,
    #               wordcloud2Output("cloud"))
  )
)


server <- function(input, output) {
  
  # HEATMAP
  output$map <- renderLeaflet({
    lang = languages %>% filter(name == input$lang)
    
    dat <- data %>% 
      filter(Latitude < 44) %>% 
      filter(Latitude > 35) %>% 
      filter(Longitude > -12) %>% 
      filter(Longitude < 7) %>% 
      filter(Tweet.language..ISO.639.1. == lang$code) %>% 
      filter(Date >= input$dateSlider[1]) %>%
      filter(Date <= input$dateSlider[2]) %>% 
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
  
  # WORDCLOUD
  terms <- reactive({
    # Change when the "update" button is pressed...
    input$update
    # ...but not for anything else
    isolate({
      withProgress({
        setProgress(message = "Processing corpus...")
        getTermMatrix(input$lang, input$dateSlider[1], input$dateSlider[2])
      })
    })
  })
  
  
  output$cloud <- renderWordcloud2({
    wordcloud2(data=terms(), size = 0.7, shape = 'pentagon')
  })
}


shinyApp(ui = ui, server = server)