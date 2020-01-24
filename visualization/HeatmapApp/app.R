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
library("raster")


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

data <- read.csv("../data/clean-twitter-data.csv")
data$Date


ui <- dashboardPage(
  dashboardHeader(  ),
  dashboardSidebar(
    textInput("wordFilter", "Filter by word: ", value = ""),
    selectInput("lang", "Choose a language:",
                choices = languages$code),
    sliderInput("opacity", label = "Opacity",
                min = 0, max = 1,
                value = 0.8),
    sliderInput("gridSize", label = "Grid Size",
                min = 1, max = 2000,
                value = 100),
    sliderInput("bandwidth", label = "Bandwidth",
                min = 0.000001, max = 0.5,
                value = 0.001),
    actionButton("update", "Change")
  ),
  dashboardBody(
    tags$style(type = "text/css", "#map {height: calc(100vh - 80px) !important;}"),
    leafletOutput("map"),
    
      )
)


server <- function(input, output) {
  
  output$map <- renderLeaflet({
    dat <- data %>% 
      filter(Latitude < 44) %>% 
      filter(Latitude > 35) %>% 
      filter(Longitude > -12) %>% 
      filter(Longitude < 7) %>% 
      filter(Tweet.language..ISO.639.1. == input$lang) %>% 
      #filter(Date >= input$dateSlider[1]) %>%
      #filter(Date <= input$dateSlider[2]) %>% 
      filter(grepl(tolower(input$wordFilter),tolower(Tweet.content)))
    
    dat <- data.table(dat)
    kde <- bkde2D(dat[ , list(Longitude, Latitude)],
                  bandwidth=c(input$bandwidth, input$bandwidth), 
                  gridsize = c(input$gridSize,input$gridSize))
    
    KernelDensityRaster <- raster(list(x=kde$x1 ,y=kde$x2 ,z = kde$fhat))
    KernelDensityRaster@data@values[which(KernelDensityRaster@data@values < 0.001)] <- NA
    palRaster <- colorNumeric(palette = viridis::viridis(n = 500), domain = KernelDensityRaster@data@values, na.color = "transparent")
    
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
}


shinyApp(ui = ui, server = server)