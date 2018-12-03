library(ROAuth)
library(tweetscores)
library(tools)

popular_accs = c("UKenyatta", "CapitalFMKenya", "KoinangeJeff", "dailynation", "LarryMadowo",
  "KTNKenya", "citizentvkenya", "ntvkenya", "RailaOdinga", "WilliamsRuto")

church_accs = c("AllNairobi", "dcumoja", "ebcnairobi", "FGCKBuruburu", "basilicanrb",
  "JPCCKenya", "JccKenya", "mavunodowntown", "mavunochurchorg", "mavunomashariki",
  "NairobiChapel", "ParkieBaptist", "ncckkenya", "winnersnairobi", "HouseofGraceHQ",
  "BuruBaptistC", "dclmhq", "DCLangata", "olqpsouthb", "IGAC_Uthiru", "HurumaRgc",
  "HolyTrinityKILE", "JBAChurch", "KVCKenya", "NairobiBaptist")

get_followers = function(twitter_users, outfolder, outfile) {
  users <- getUsersBatch(screen_names = twitter_accs, oauth = "oauth")
  names(users)[names(users) == "name"] <- "twitter_name"
  accounts <- users$screen_name[users$followers_count > 0]

  accounts.done <- gsub(".rdata", "", list.files("data/followers_lists_church"))
  accounts.left <- accounts[accounts %in% accounts.done == FALSE]
  accounts.left <- accounts.left[!is.na(accounts.left)]

  while (length(accounts.left) > 0) {
    # sample randomly one account to get followers new.user <- sample(accounts.left,
    # 1)
    new.user <- accounts.left[1]
    cat(new.user, "---", users$followers_count[users$screen_name == new.user],
      " followers --- ", length(accounts.left), " accounts left!\n")

    # download followers (with some exception handling...)
    error <- tryCatch(followers <- getFollowers(screen_name = new.user, oauth = "oauth",
      sleep = 0.5, verbose = FALSE), error = function(e) e)
    if (inherits(error, "error")) {
      cat("Error! On to the next one...")
      next
    }

    # save to file and remove from lists of 'accounts.left'
    file.name <- paste0("data/followers_lists_church/", new.user, ".rdata")
    save(followers, file = file.name)
    accounts.left <- accounts.left[-which(accounts.left %in% new.user)]
  }

  fls = list.files(outfolder, full.names = TRUE)

  followers.list = list(NULL)
  for (i in 1:length(fls)) {
    load(fls[i])
    account = basename(file_path_sans_ext(fls[i]))
    filename = paste0("/media/data/PhD_Projects/kenya/data/interim/church_txt/", 
      account, ".txt")
    write(followers, filename)
    followers.list[[i]] = followers
    cat(i, "of", length(fls), "\n")
  }

  all = unlist(followers.list)
  all = unique(all)
  length(all)

  write(all, outfile)
}

get_followers(popular_accs, "/media/data/PhD_Projects/kenya/data/interim/followers_popular",
  "/media/data/PhD_Projects/kenya/data/interim/users_popular.txt")
get_followers(church_accs, "/media/data/PhD_Projects/kenya/data/interim/followers_church",
  "/media/data/PhD_Projects/kenya/data/interim/users_church.txt")
