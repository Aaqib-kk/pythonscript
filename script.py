from pymongo import MongoClient
from bson.objectid import ObjectId
import time
from datetime import datetime, timedelta

db_name = "tasks_db"
host_name = "mongodb+srv://***:***@chap-chap.atxtme7.mongodb.net/python"

PY_CLIENT = MongoClient(host_name)
PY_DB = PY_CLIENT[db_name]
SELLER_COLLECTION = PY_DB["productSellers"]
PRODUCT_COLLECTION = PY_DB["products"]
PRICE_HISTORY_COLLECTION = PY_DB["productPriceHistory"]
PRODUCT_CATEGORY_COLLECTION = PY_DB["productCategory"]
APP_USER_BROWSING_HISTORY = PY_DB["app_UserBrowsingHistory"]


def change_objectid_to_str(products):
    for product in products:
        product["_id"] = str(product["_id"])
        product["productSeller"]["_id"] = str(product["productSeller"]["_id"])
        product["productCategoryId"] = str(product["productCategoryId"])
        if type(product["aggregationId"]) != int:
            product["aggregationId"] = str(product["aggregationId"])
        if product.get("connectedProducts"):
            for related_product in product["connectedProducts"]:
                related_product["aggregationId"] = str(related_product["aggregationId"])
                related_product["_id"] = str(related_product["_id"])
                related_product["productSeller"]["_id"] = str(related_product["productSeller"]["_id"])
                related_product["productCategoryId"] = str(related_product["productCategoryId"])
                if related_product.get("productPrice"):
                    if related_product["productPrice"].get("_id"):
                        related_product["productPrice"]["_id"] = str(related_product["productPrice"]["_id"])
                    if related_product["productPrice"].get("productId"):
                        related_product["productPrice"]["productId"] = str(related_product["productPrice"]["productId"])
                    if related_product["productPrice"].get("sellerId"):
                        related_product["productPrice"]["sellerId"] = str(related_product["productPrice"]["sellerId"])
                if related_product.get("variantId") is None:
                    if related_product.get("productVariants"):
                        related_product.pop("productVariants")
        if product.get("variantId") is None:
            if product.get("productVariants"):
                product.pop("productVariants")
        if product.get("isOnTrack") is None:
            product["isOnTrack"] = False
        else:
            if product["isOnTrack"]:
                product["isOnTrack"] = True
            else:
                product["isOnTrack"] = False
        if product.get("productPrice"):
            if product["productPrice"].get("_id"):
                product["productPrice"]["_id"] = str(product["productPrice"]["_id"])
            if product["productPrice"].get("productId"):
                product["productPrice"]["productId"] = str(product["productPrice"]["productId"])
            if product["productPrice"].get("sellerId"):
                product["productPrice"]["sellerId"] = str(product["productPrice"]["sellerId"])
        for product_price in product["priceHistory"]:
            if product_price.get("_id"):
                product_price["_id"] = str(product_price["_id"])
            if product_price.get("productId"):
                product_price["productId"] = str(product_price["productId"])
            if product_price.get("sellerId"):
                product_price["sellerId"] = str(product_price["sellerId"])

    return products


def get_product_data(category_id, sellers, brands, search_query, productUPC, sort_by, discounted, coupon, free_delivery,
                     min_price, max_price, ):
    """
    The function `get_product_data` retrieves product data based on various search criteria such as
    category, sellers, brands, search query, UPC, sorting options, discounts, coupons, price range,
    and more.

    :param category_id: The `category_id` parameter is used to specify the category ID for filtering
    products. It helps to narrow down the search to products within a specific category
    :param sellers: The `sellers` parameter in the `get_product_data` function is used to filter
    products based on the sellers specified in the input. The function will retrieve products that are
    sold by the sellers provided in the `sellers` list
    :param brands: The `brands` parameter in the `get_product_data` function is used to filter
    products based on the brand names provided in the list. The function will search for products that
    match any of the brand names specified in the `brands` list
    :param search_query: The `search_query` parameter is used to specify a search term or keyword that
    you want to use to search for products. It can be a string representing the search query that you
    want to use to filter the products
    :param productUPC: The `productUPC` parameter in the `get_product_data` function is used to search
    for a product based on its Universal Product Code (UPC). If a UPC is provided, the function will
    include a search condition in the aggregation pipeline to find the product with that specific UPC.
    If a
    :param sort_by: Sort_by parameter is used to specify the sorting criteria for the products in the
    search results. It can include options like sorting by price, rating, popularity, etc
    :param discounted: Discounted is a boolean parameter that indicates whether to include only
    discounted products in the search results. If set to True, only products with discounts will be
    included in the search results. If set to False, all products will be included regardless of
    whether they are discounted or not
    :param coupon: The `coupon` parameter in the `get_product_data` function is used to filter
    products based on whether they have a coupon available for them. If `coupon` is set to `True`, the
    function will include only products that have a coupon available. If `coupon` is set to `False
    :param free_delivery: The `free_delivery` parameter in the `get_product_data` function is used to
    filter products based on whether they offer free delivery or not. If `free_delivery` is set to
    `True`, the function will include only products that offer free delivery. If set to `False`, it
    will include
    :param min_price: The `min_price` parameter in the `get_product_data` function represents the
    minimum price range for the products you are searching for. It is used to filter out products that
    fall below this minimum price threshold
    :param max_price: The `max_price` parameter in the `get_product_data` function represents the
    maximum price limit for the products you want to retrieve. This parameter is used to filter out
    products that are priced above this maximum value
    """
    start_time = time.perf_counter()
    category_id = category_id
    sellers = sellers
    brands = brands
    search_query = search_query
    productUPC = productUPC
    sort_by = sort_by
    discounted = discounted
    coupon = coupon
    free_delivery = free_delivery
    min_price = min_price
    max_price = max_price

    if min_price < 0 or max_price < 0:
        print("Invalid request format or parameters")
        return
    if min_price > max_price > 0:
        print("Invalid request format or parameters")
        return
    if min_price > max_price > 0 and min_price > 0:
        print("Invalid request format or parameters")
        return
    aggregation = []
    if productUPC:
        aggregation.append(
            {
                "$search": {
                    "index": "autocomplete_product_search",
                    "compound": {
                        "should": [
                            {
                                "autocomplete": {
                                    "query": productUPC,
                                    "path": "productUPC",
                                },
                            },
                        ],
                    },
                },
            },
        )
        aggregation.append(
            {
                "$limit": 1,
            },
        )

    else:
        if search_query:
            aggregation.append(
                {
                    "$search": {
                        "index": "autocomplete_product_search",
                        "compound": {
                            "should": [
                                {
                                    "autocomplete": {
                                        "query": search_query,
                                        "path": "productTitle",
                                    },
                                },
                                {
                                    "autocomplete": {
                                        "query": search_query,
                                        "path": "productUPC",
                                    },
                                },
                                {
                                    "autocomplete": {
                                        "query": search_query,
                                        "path": "productModel",
                                    },
                                },
                            ],
                        },
                    },
                }
            )
        if sellers:
            active_seller_filter = []
            seller_match_condition = {"$match": {"$or": []}}

            def get_seller_id(seller_name):
                seller = SELLER_COLLECTION.find_one({"sellerName": seller_name})
                if not seller:
                    return None
                return {"sellerId": seller["_id"]}, {
                    "seller": {
                        "_id": str(seller["_id"]),
                        "sellerName": seller["sellerName"],
                    }
                }

            for seller_name in sellers:
                if len(seller_name.strip()) == 0:
                    continue
                seller_match, seller_information = get_seller_id(seller_name.strip())
                if seller_match is None:
                    print("No seller by this name")
                    return
                seller_match_condition["$match"]["$or"].append(seller_match)
                active_seller_filter.append(seller_information)
            if active_seller_filter:
                aggregation.append(seller_match_condition)
        category_for_third_pipeline = None
        if category_id:

            def get_categories(category_id):
                category_extracted = {"$match": {"$or": []}}
                aggregation_forward = [
                    {
                        "$match": {
                            "_id": category_id,
                        },
                    },
                    {
                        "$graphLookup": {
                            "from": "productCategory",
                            "startWith": category_id,
                            "connectFromField": "_id",
                            "connectToField": "parentId",
                            "as": "data",
                        },
                    },
                ]
                forward = list(
                    PRODUCT_CATEGORY_COLLECTION.aggregate(aggregation_forward)
                )[0]
                if forward is None:
                    return None, None
                active_filter_return = []
                if forward["data"]:
                    for item in forward["data"]:
                        category_extracted["$match"]["$or"].append(
                            {"productCategoryId": item["_id"]}
                        )

                else:
                    category_extracted["$match"]["$or"].append(
                        {"productCategoryId": forward["_id"]}
                    )
                active_filter_return.append(
                    {
                        "category": {
                            "categoryId": str(forward["_id"]),
                            "categoryName": forward["categoryName"],
                        }
                    }
                )

                return category_extracted, active_filter_return

            category_extracted, filter_return = get_categories(ObjectId(category_id))
            if category_extracted is None and filter_return is None:
                print("No category by this id")
                return
            aggregation.append(category_extracted)
            category_for_third_pipeline = category_extracted

        if brands:
            active_brand_filters = []

            def get_brand_match(brand_name):
                return {"productBrand": {"$regex": brand_name, "$options": "i"}}

            match_array = {"$match": {"$or": []}}
            for brand_name in brands:
                if len(brand_name.strip()) == 0:
                    continue
                match_array["$match"]["$or"].append(get_brand_match(brand_name.strip()))
                active_brand_filters.append(brand_name)

            if active_brand_filters:
                aggregation.append(match_array)
    limit = 50
    if not search_query and category_id:
        limit = 500
        print('Not search query but category is provided')
    elif search_query:
        print('Search query is provided')
        limit = 50

    # Add the includeVariants condition
    aggregation = aggregation_extend_include_variants(
        aggregation,
        sort_by,
        category_for_third_pipeline,
        None,
        discounted,
        coupon,
        free_delivery,
        min_price,
        max_price,
        limit=limit
    )
    products = list(PRODUCT_COLLECTION.aggregate(aggregation))
    products = change_objectid_to_str(products)
    duration_seconds = time.perf_counter() - start_time
    duration = timedelta(seconds=duration_seconds)
    # Format the duration
    hours, remainder = divmod(duration_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    formatted_duration = "{:02}:{:02}:{:06.6f}".format(
        int(hours), int(minutes), seconds
    )

    return products, formatted_duration


def aggregation_extend_include_variants(aggregation, sort_type=None, category_list=None, user_id=None, discounted=0,
                                        coupon=0,
                                        free_delivery=0, min_price=0, max_price=0, format_price=None, limit=500,
                                        only_category=False):
    """
    The function `aggregation_extend_include_variants` constructs a MongoDB aggregation pipeline for
    retrieving product data with various filters and sorting options.

    :param aggregation: The `aggregation_extend_include_variants` function takes in several parameters
    to build an aggregation pipeline for MongoDB queries. Here's a brief explanation of each parameter:
    :param sort_type: The `sort_type` parameter in the `aggregation_extend_include_variants` function is
    used to specify the sorting type for the results. It can be used to determine how the products
    should be ordered in the final output based on certain criteria such as price, rating, or relevance
    :param category_list: The `category_list` parameter is a list of categories used for filtering
    products in the aggregation. It allows you to specify one or more categories to narrow down the
    search results to products that belong to those categories
    :param user_id: The `user_id` parameter in the `aggregation_extend_include_variants` function is
    used to filter the results based on a specific user's tracking products. If a `user_id` is provided,
    the function will include a `` stage in the aggregation pipeline to check if the product is
    :param discounted: The `discounted` parameter in the `aggregation_extend_include_variants` function
    is used to filter products based on whether they have a discount applied. When `discounted` is set
    to 1, the aggregation pipeline will include a match stage to filter products that have an existing
    `productOld, defaults to 0 (optional)
    :param coupon: The `coupon` parameter in the `aggregation_extend_include_variants` function is used
    to filter products based on whether they have a coupon associated with them. If `coupon` is set to
    1, the aggregation pipeline will include a match stage to filter products that have a
    `productCoupon`, defaults to 0 (optional)
    :param free_delivery: The `free_delivery` parameter in the `aggregation_extend_include_variants`
    function is used to filter products based on whether they offer free delivery or not. When
    `free_delivery` is set to 1, the aggregation pipeline will include a `` stage to filter
    products where the `product, defaults to 0 (optional)
    :param min_price: The `min_price` parameter in the `aggregation_extend_include_variants` function is
    used to filter products based on their minimum price. If a `min_price` value is provided, the
    aggregation pipeline will include a stage to filter out products with a price lower than
    the specified minimum, defaults to 0 (optional)
    :param max_price: The `max_price` parameter in the `aggregation_extend_include_variants` function is
    used to filter products based on their maximum price. If a `max_price` value is provided, the
    function will include a  stage in the aggregation pipeline to filter out products with a
    price greater than, defaults to 0 (optional)
    :param format_price: The `format_price` parameter in the `aggregation_extend_include_variants`
    function is used to specify the format in which the price history should be formatted before
    returning the results. This parameter is passed to the `price_history_formater` function to format
    the price history data accordingly
    :return: The function `aggregation_extend_include_variants` returns the aggregation pipeline with
    various stages added based on the input parameters such as sort type, category list, user ID,
    discounts, coupons, free delivery, min and max prices, and format price. The aggregation pipeline
    includes stages for matching, setting scores, facetting with and without variants, projecting
    fields, unwinding arrays, looking up related data,
    """
    connected_products_pipeline = get_connected_pipeline(category_list, product_list_endpoint=True)
    aggregation.extend([
        {
            '$match':
                {
                    "stockStatus.stockStatus": {
                        '$ne': 0,
                    },
                },
        },
        {
            '$set':
                {
                    'score': {
                        '$meta': "searchScore",
                    },
                },
        },
        {
            '$facet':
                {
                    'withVariants': [
                        {
                            '$match': {
                                'productVariants': {
                                    '$exists': True
                                }
                            }
                        },
                        {
                            '$group': {
                                '_id': "$productVariants.variantId",
                                'mainProduct': {
                                    '$first': "$$ROOT"
                                }
                            }
                        },
                        {
                            '$replaceRoot': {
                                'newRoot': "$mainProduct"
                            }
                        }
                    ],
                    'withoutVariants': [
                        {
                            '$match': {
                                'productVariants': {
                                    '$exists': False
                                }
                            }
                        }
                    ]
                }
        },
        {
            '$project':
                {
                    'combinedResults': {
                        '$concatArrays': [
                            "$withoutVariants",
                            "$withVariants"
                        ]
                    }
                }
        },
        {
            '$unwind': "$combinedResults"
        },
        {
            '$replaceRoot':
                {
                    'newRoot': "$combinedResults"
                }
        },
        {
            '$facet':
                {
                    'withAggregation': [
                        {
                            '$match': {
                                'aggregationId': {
                                    '$ne': 0,
                                    '$type': "objectId"
                                }
                            }
                        },
                        {
                            '$group': {
                                '_id': "$aggregationId",
                                'mainProduct': {
                                    '$first': "$$ROOT"
                                }
                            }
                        },
                        {
                            '$replaceRoot': {
                                'newRoot': "$mainProduct"
                            }
                        }
                    ],
                    'withoutAggregation': [
                        {
                            '$match': {
                                'aggregationId': 0
                            }
                        }
                    ]
                }
        },
        {
            '$project':
                {
                    'aggregationFiltered': {
                        '$concatArrays': [
                            "$withAggregation",
                            "$withoutAggregation"
                        ]
                    }
                }
        },
        {
            '$unwind': "$aggregationFiltered"
        },
        {
            '$replaceRoot':
                {
                    'newRoot': "$aggregationFiltered"
                }
        },
        {
            '$limit': limit,
        },
        {
            '$facet':
                {
                    'withVariants': [
                        {
                            '$match': {
                                'productVariants': {
                                    '$exists': True,
                                },
                            },
                        },
                        {
                            '$lookup': {
                                'from': "products",
                                'localField': "productVariants.variantId",
                                'foreignField': "productVariants.variantId",
                                'as': "relatedProducts",
                            },
                        },
                        {
                            '$unwind': {
                                'path': "$relatedProducts",
                                'preserveNullAndEmptyArrays': True,
                            },
                        },
                        {
                            '$match': {
                                "relatedProducts.stockStatus.stockStatus":
                                    {
                                        '$ne': 0,
                                    },
                            },
                        },
                        {
                            '$group': {
                                '_id': "$productVariants.variantId",
                                'mainProduct': {
                                    '$first': "$$ROOT",
                                },
                                'relatedProducts': {
                                    '$addToSet': "$relatedProducts",
                                },
                                'firstVariant': {
                                    '$first': "$$ROOT.productVariants.variantDetails",
                                },
                            },
                        },
                        {
                            '$project': {
                                'mainProduct': 1,
                                'relatedProducts': {
                                    '$concatArrays': [
                                        ["$firstVariant"],
                                        {
                                            '$filter': {
                                                'input': "$relatedProducts.productVariants.variantDetails",
                                                'cond': {
                                                    '$ne': [
                                                        "$$this",
                                                        "$firstVariant",
                                                    ],
                                                },
                                            },
                                        },
                                    ],
                                },
                            },
                        },
                        # New color
                        {
                            '$unwind':
                                {
                                    'path': "$relatedProducts",
                                },
                        },
                        {
                            '$lookup': {
                                'from': "productColorCode",
                                'let': {
                                    'color1': "$relatedProducts.Color",
                                    'color2': "$relatedProducts.color",
                                    'color3': "$relatedProducts.COLOR",
                                },
                                'pipeline': [
                                    {
                                        '$match': {
                                            '$expr': {
                                                '$or': [
                                                    {

                                                        '$eq': [
                                                            {
                                                                '$toUpper': "$color",
                                                            },
                                                            {
                                                                '$toUpper': "$$color1",
                                                            },
                                                        ],
                                                    },
                                                    {
                                                        '$eq': [
                                                            {
                                                                '$toUpper': "$color",
                                                            },
                                                            {
                                                                '$toUpper': "$$color2",
                                                            },
                                                        ],
                                                    },
                                                    {
                                                        '$eq': [
                                                            {
                                                                '$toUpper': "$color",
                                                            },
                                                            {
                                                                '$toUpper': "$$color3",
                                                            },
                                                        ],
                                                    },
                                                ],
                                            },
                                        },
                                    },
                                    {
                                        '$project': {
                                            '_id': 0,
                                            'hexCode': "$hexCode",
                                        },
                                    },
                                ],
                                'as': "relatedProducts.hexCode",
                            },
                        },
                        {
                            '$unwind': {
                                'path': "$relatedProducts.hexCode",
                                'preserveNullAndEmptyArrays': True,
                            },
                        },
                        {
                            '$addFields': {
                                "relatedProducts.hexCode":
                                    "$relatedProducts.hexCode.hexCode",
                            },
                        },
                        {
                            '$group': {
                                '_id': "$_id",
                                'mainProduct': {
                                    '$first': "$mainProduct",
                                },
                                'relatedProducts': {
                                    '$push': "$relatedProducts",
                                },
                            },
                        },
                        # new color end
                        {
                            '$project': {
                                '_id': "$mainProduct._id",
                                'productTitle': "$mainProduct.productTitle",
                                'productLink': "$mainProduct.productLink",
                                'productDescription': "$mainProduct.productDescription",
                                'imageLink': "$mainProduct.imageLink",
                                'productLocalId': "$mainProduct.productLocalId",
                                'productBrand': "$mainProduct.productBrand",
                                'sellerName': "$mainProduct.sellerName",
                                'stockStatus': "$mainProduct.stockStatus",
                                'userRating': "$mainProduct.userRating",
                                'productUPC': "$mainProduct.productUPC",
                                'productModel': "$mainProduct.productModel",
                                'sellerId': "$mainProduct.sellerId",
                                'productCategoryId': "$mainProduct.productCategoryId",
                                'lastUpdate': "$mainProduct.lastUpdate",
                                'aggregationId': "$mainProduct.aggregationId",
                                'variantId': "$mainProduct.productVariants.variantId",
                                'relatedProducts': "$relatedProducts",
                                'score': "$mainProduct.score"
                            },
                        },
                    ],
                    'withoutVariants': [
                        {
                            '$match': {
                                'productVariants': {
                                    '$exists': False,
                                },
                            },
                        },
                        {
                            '$project': {
                                '_id': 1,
                                'productTitle': 1,
                                'productLink': 1,
                                'productDescription': 1,
                                'imageLink': 1,
                                'productLocalId': 1,
                                'productBrand': 1,
                                'sellerName': 1,
                                'stockStatus': 1,
                                'userRating': 1,
                                'productUPC': 1,
                                'productModel': 1,
                                'sellerId': 1,
                                'productCategoryId': 1,
                                'lastUpdate': 1,
                                'aggregationId': 1,
                                'score': 1,
                            },
                        },
                    ],
                },
        },
        {
            '$project':
                {
                    'combinedResults': {
                        '$concatArrays': [
                            "$withoutVariants",
                            "$withVariants",
                        ],
                    },
                },
        },
        {
            '$unwind': "$combinedResults",
        },
        {
            '$replaceRoot':
                {
                    'newRoot': "$combinedResults",
                },
        },
        {
            '$lookup':
                {
                    'from': "productPriceHistory",
                    'localField': "_id",
                    'foreignField': "productId",
                    'as': "priceHistory",
                },
        },
        {
            '$unwind':
                {
                    'path': "$priceHistory",
                    'preserveNullAndEmptyArrays': True,
                },
        },
        {
            '$sort':
                {
                    "priceHistory.priceUpdateTime": -1,
                },
        },
        {
            '$group':
                {
                    '_id': "$_id",
                    'root': {
                        '$first': "$$ROOT",
                    },
                    'priceHistory': {
                        '$push': "$priceHistory",
                    },
                },
        },
        {
            '$addFields':
                {
                    "root.priceHistory": {
                        '$slice': ["$priceHistory", 10],
                    },
                    "root.productPrice": {
                        '$arrayElemAt': ["$priceHistory", 0],
                    },
                },
        },
        {
            '$addFields':
                {
                    "root.productPrice": {
                        '$let': {
                            'vars': {
                                'keysToRemove': [
                                    "productId",
                                    "sellerId",
                                ],
                            },
                            'in': {
                                '$arrayToObject': {
                                    '$filter': {
                                        'input': {
                                            '$objectToArray': "$root.productPrice",
                                        },
                                        'cond': {
                                            '$not': {
                                                '$in': [
                                                    "$$this.k",
                                                    "$$keysToRemove",
                                                ],
                                            },
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
        },
        {
            '$lookup':
                {
                    'from': "productSellers",
                    'localField': "root.sellerId",
                    'foreignField': "_id",
                    'as': "sellerData",
                },
        },
        {
            '$unwind':
                {
                    'path': "$sellerData",
                },
        },
        {
            '$project':
                {
                    'score': "$root.score",
                    '_id': "$root._id",
                    'productTitle': "$root.productTitle",
                    'productLink': "$root.productLink",
                    'productDescription': "$root.productDescription",
                    'imageLink': "$root.imageLink",
                    'productLocalId': "$root.productLocalId",
                    'productBrand': "$root.productBrand",
                    'sellerName': "$root.sellerName",
                    'stockStatus': "$root.stockStatus",
                    'userRating': "$root.userRating",
                    'productUPC': "$root.productUPC",
                    'productModel': "$root.productModel",
                    'productSeller': {
                        '_id': "$sellerData._id",
                        'sellerName': "$sellerData.sellerName",
                    },
                    'productCategoryId': "$root.productCategoryId",
                    'lastUpdate': "$root.lastUpdate",
                    'aggregationId': "$root.aggregationId",
                    'productPrice': "$root.productPrice",
                    'priceHistory': price_history_formater(format_price),
                    'variantId': "$root.variantId",
                    'productVariants': "$root.relatedProducts",
                },
        },
        {
            '$facet':
                {
                    'withObjectId': [
                        {
                            '$match': {
                                'aggregationId': {
                                    '$ne': 0,
                                    '$type': "objectId",
                                },
                            },
                        },
                        {
                            '$lookup': {
                                'from': "products",
                                'localField': "aggregationId",
                                'foreignField': "aggregationId",
                                'pipeline': connected_products_pipeline,
                                'as': "connectedProducts",
                            },
                        },
                        {
                            '$addFields': {
                                'smallest': {
                                    '$arrayElemAt': [
                                        "$connectedProducts",
                                        0,
                                    ],
                                },
                            },
                        },
                        {
                            '$project': {
                                'score': 1,
                                '_id': "$smallest._id",
                                'productTitle': "$smallest.productTitle",
                                'productLink': "$smallest.productLink",
                                'productDescription': "$smallest.productDescription",
                                'imageLink': "$smallest.imageLink",
                                'productLocalId': "$smallest.productLocalId",
                                'productBrand': "$smallest.productBrand",
                                'sellerName': "$smallest.sellerName",
                                'stockStatus': "$smallest.stockStatus",
                                'userRating': "$smallest.userRating",
                                'productUPC': "$smallest.productUPC",
                                'productModel': "$smallest.productModel",
                                'productSeller': "$smallest.productSeller",
                                'productCategoryId': "$smallest.productCategoryId",
                                'lastUpdate': "$smallest.lastUpdate",
                                'aggregationId': "$smallest.aggregationId",
                                'productPrice': "$smallest.productPrice",
                                'priceHistory': "$smallest.priceHistory",
                                'variantId': "$smallest.variantId",
                                'productVariants': "$smallest.productVariants",
                                'connectedProducts': "$connectedProducts",
                            },
                        },
                        {
                            '$addFields': {
                                'connectedProducts': {
                                    '$filter': {
                                        'input': "$connectedProducts",
                                        'cond': {
                                            '$or': [
                                                {'$ne': ["$$this._id", "$_id"]},
                                                {'$ne': ["$$this.variantId", "$variantId"]}
                                            ]
                                        },
                                    },
                                },
                            },
                        },
                        {
                            '$group': {
                                '_id': "$aggregationId",
                                'rootDocument': {
                                    '$first': "$$ROOT",
                                },
                            },
                        },
                        {
                            '$replaceRoot': {
                                'newRoot': "$rootDocument",
                            },
                        },
                    ],
                    'withoutObjectId': [
                        {
                            '$match': {
                                'aggregationId': 0,
                            },
                        },
                    ],
                },
        },
        {
            '$project': {
                'combinedResults': {
                    '$concatArrays': [
                        "$withoutObjectId",
                        "$withObjectId",
                    ],
                },
            },
        },
        {
            '$unwind': "$combinedResults",
        },
        {
            '$replaceRoot': {
                'newRoot': "$combinedResults",
            },
        },

    ]
    )

    if user_id:
        aggregation.extend(
            [
                {
                    "$lookup": {
                        "from": "app_UserTrackingProducts",
                        "localField": "_id",
                        "foreignField": "productId",
                        "pipeline": [{"$match": {"userId": user_id}}],
                        "as": "isOnTrack",
                    },
                }
            ]
        )
    if free_delivery == 1:
        aggregation.extend(
            [
                {
                    "$match": {
                        "productPrice.productShippingFee": "Free Delivery",
                    },
                }
            ]
        )
    if discounted == 1:
        aggregation.extend(
            [
                {
                    "$match": {
                        "productPrice.productOldPrice": {
                            "$exists": True,
                        },
                    }
                },
            ]
        )
    if coupon == 1:
        aggregation.extend(
            [
                {
                    "$match": {
                        "productPrice.productCoupon": {
                            "$exists": True,
                        },
                    },
                }
            ]
        )
    if min_price > 0:
        aggregation.extend(
            [
                {
                    "$match": {
                        "productPrice.productPrice": {
                            "$gte": min_price,
                        },
                    },
                }
            ]
        )
    if max_price > 0:
        aggregation.extend(
            [
                {
                    "$match": {
                        "productPrice.productPrice": {
                            "$lte": max_price,
                        },
                    }
                }
            ]
        )
    if sort_type:
        aggregation.extend(get_sort_type(sort_type))
    return aggregation


def get_sort_type(sort_type):
    """
    The function `get_sort_type` determines the sorting criteria based on the input `sort_type` for a
    MongoDB query.

    :param sort_type: The `sort_type` parameter can have the following values:
    :return: The `get_sort_type` function returns a list of MongoDB aggregation pipeline stages based on
    the `sort_type` parameter provided.
    """
    if sort_type == "related":
        return [
            {
                "$sort": {
                    "score": -1,
                },
            }
        ]
    elif sort_type == "acc_price" or sort_type == "dec_price":
        return [
            {
                "$sort": {
                    "productPrice.productPrice": 1 if sort_type == "acc_price" else -1
                },
            }
        ]
    elif sort_type == "acc_rating" or sort_type == "dec_rating":
        return [
            {
                "$addFields": {
                    "ratingMatch": {
                        "$regexFind": {
                            "input": "$userRating.ratingStars",
                            "regex": "^[0-9.]+",
                        },
                    },
                },
            },
            {
                "$addFields": {
                    "rating": {
                        "$toDouble": "$ratingMatch.match",
                    },
                },
            },
            {
                "$sort": {
                    "rating": 1 if sort_type == "acc_rating" else -1,
                },
            },
            {
                "$unset": ["ratingMatch", "rating"],
            },
        ]


def get_connected_pipeline(get_category_list, product_list_endpoint=False):
    """
    The function `get_connected_pipeline` constructs a MongoDB aggregation pipeline to retrieve product
    data with price history and related products, optionally filtering by category.

    :param get_category_list: The `get_category_list` parameter is a function that is used to retrieve a
    list of categories for filtering the pipeline results
    :param product_list_endpoint: The `product_list_endpoint` parameter in the `get_connected_pipeline`
    function is a boolean parameter with a default value of `False`. It is used to determine whether to
    include the product list endpoint in the pipeline. If set to `True`, the product list endpoint will
    be included in the pipeline, defaults to False (optional)
    :return: The function `get_connected_pipeline` returns a list of pipeline stages for aggregating and
    processing data in a MongoDB collection. These stages are used to
    fetch and manipulate data related to product prices, stock status, seller information, product
    variants, color
    """

    def return_category_list(category_list):
        if category_list:
            return category_list
        return None

    category = return_category_list(get_category_list)

    connected_pipeline = [
        {
            '$lookup': {
                'from': "productPriceHistory",
                'localField': "_id",
                'foreignField': "productId",
                'as': "priceHistory",
            },
        },
        {
            '$unwind': {
                'path': "$priceHistory",
                'preserveNullAndEmptyArrays': True,
            },
        },
        {
            '$sort': {
                "priceHistory.priceUpdateTime": -1,
            },
        },
        {
            '$group': {
                '_id': "$_id",
                'root': {
                    '$first': "$$ROOT",
                },
                'priceHistory': {
                    '$push': "$priceHistory",
                },
            },
        },
        {
            '$addFields': {
                "root.productPrice": {
                    '$arrayElemAt': [
                        "$priceHistory",
                        0,
                    ],
                },
            },
        },
        {
            '$addFields': {
                "root.productPrice": {
                    '$let': {
                        'vars': {
                            'keysToRemove': [
                                "productId",
                                "sellerId",
                            ],
                        },
                        'in': {
                            '$arrayToObject': {
                                '$filter': {
                                    'input': {
                                        '$objectToArray': "$root.productPrice",
                                    },
                                    'cond': {
                                        '$not': {
                                            '$in': [
                                                "$$this.k",
                                                "$$keysToRemove",
                                            ],
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
        {
            '$match': {
                "root.stockStatus.stockStatus":
                    {
                        '$ne': 0,
                    },
            },
        },
        {
            '$project': {
                'score': "$root.score",
                '_id': "$root._id",
                'productTitle': "$root.productTitle",
                'productLink': "$root.productLink",
                'productDescription': "$root.productDescription",
                'imageLink': "$root.imageLink",
                'productLocalId': "$root.productLocalId",
                'productBrand': "$root.productBrand",
                'sellerName': "$root.sellerName",
                'stockStatus': "$root.stockStatus",
                'userRating': "$root.userRating",
                'productUPC': "$root.productUPC",
                'productModel': "$root.productModel",
                'sellerId': "$root.sellerId",
                'productCategoryId': "$root.productCategoryId",
                'lastUpdate': "$root.lastUpdate",
                'aggregationId': "$root.aggregationId",
                'productPrice': "$root.productPrice",
                'priceHistory': {
                    '$map': {
                        'input': "$priceHistory",
                        'as': "price",
                        'in': {
                            'productPrice': "$$price.productPrice",
                            'priceUpdateTime': "$$price.priceUpdateTime",
                        },
                    },
                },
                'variantId': "$root.productVariants.variantId",
                'productVariants': "$root.productVariants",
            },
        },
        {
            '$lookup': {
                'from': "products",
                'localField': "variantId",
                'foreignField': "productVariants.variantId",
                'as': "relatedProducts",
            },
        },
        {
            '$unwind': {
                'path': "$relatedProducts",
                'preserveNullAndEmptyArrays': True,
            },
        },
        {
            '$match': {
                "relatedProducts.stockStatus.stockStatus":
                    {
                        '$ne': 0,
                    },
            },
        },
        {
            '$group': {
                '_id': "$_id",
                'mainProduct': {
                    '$first': "$$ROOT",
                },
                'relatedProducts': {
                    '$addToSet': "$relatedProducts",
                },
                'firstVariant': {
                    '$first': "$$ROOT.productVariants.variantDetails",
                },
            },
        },
        {
            '$project': {
                'mainProduct': 1,
                'relatedProducts': {
                    '$concatArrays': [
                        ["$firstVariant"],
                        {
                            '$filter': {
                                'input':
                                    "$relatedProducts.productVariants.variantDetails",
                                'cond': {
                                    '$ne': [
                                        "$$this",
                                        "$firstVariant",
                                    ],
                                },
                            },
                        },
                    ],
                },
            },
        },
        # new color
        {
            '$unwind':
                {
                    'path': "$relatedProducts",
                },
        },
        {
            '$lookup': {
                'from': "productColorCode",
                'let': {
                    'color1': "$relatedProducts.Color",
                    'color2': "$relatedProducts.color",
                    'color3': "$relatedProducts.COLOR",
                },
                'pipeline': [
                    {
                        '$match': {
                            '$expr': {
                                '$or': [
                                    {

                                        '$eq': [
                                            {
                                                '$toUpper': "$color",
                                            },
                                            {
                                                '$toUpper': "$$color1",
                                            },
                                        ],
                                    },
                                    {
                                        '$eq': [
                                            {
                                                '$toUpper': "$color",
                                            },
                                            {
                                                '$toUpper': "$$color2",
                                            },
                                        ],
                                    },
                                    {
                                        '$eq': [
                                            {
                                                '$toUpper': "$color",
                                            },
                                            {
                                                '$toUpper': "$$color3",
                                            },
                                        ],
                                    },
                                ],
                            },
                        },
                    },
                    {
                        '$project': {
                            '_id': 0,
                            'hexCode': "$hexCode",
                        },
                    },
                ],
                'as': "relatedProducts.hexCode",
            },
        },
        {
            '$unwind': {
                'path': "$relatedProducts.hexCode",
                'preserveNullAndEmptyArrays': True,
            },
        },
        {
            '$addFields': {
                "relatedProducts.hexCode":
                    "$relatedProducts.hexCode.hexCode",
            },
        },
        {
            '$group': {
                '_id': "$_id",
                'mainProduct': {
                    '$first': "$mainProduct",
                },
                'relatedProducts': {
                    '$push': "$relatedProducts",
                },
            },
        },
        # new color
        {
            '$lookup': {
                'from': "productSellers",
                'localField': "mainProduct.sellerId",
                'foreignField': "_id",
                'as': "sellerData",
            },
        },
        {
            '$unwind': {
                'path': "$sellerData",
            },
        },
        {
            '$project': {
                'score': "$mainProduct.score",
                '_id': "$mainProduct._id",
                'productTitle': "$mainProduct.productTitle",
                'productLink': "$mainProduct.productLink",
                'productDescription': "$mainProduct.productDescription",
                'imageLink': "$mainProduct.imageLink",
                'productLocalId': "$mainProduct.productLocalId",
                'productBrand': "$mainProduct.productBrand",
                'sellerName': "$mainProduct.sellerName",
                'stockStatus': "$mainProduct.stockStatus",
                'userRating': "$mainProduct.userRating",
                'productUPC': "$mainProduct.productUPC",
                'productModel': "$mainProduct.productModel",
                'productSeller': {
                    '_id': "$sellerData._id",
                    'sellerName': "$sellerData.sellerName",
                },
                'productCategoryId': "$mainProduct.productCategoryId",
                'lastUpdate': "$mainProduct.lastUpdate",
                'aggregationId': "$mainProduct.aggregationId",
                'productPrice': "$mainProduct.productPrice",
                'priceHistory': "$mainProduct.priceHistory",
                'variantId': "$mainProduct.variantId",
                'productVariants': "$relatedProducts",
            },
        },
        {
            '$sort': {
                "productPrice.productPrice": 1,
            },
        },
    ]

    if category:
        connected_pipeline.insert(0, category)
    return connected_pipeline


def price_history_formater(format):
    if format:
        return "$root.priceHistory"
    return {
        "$map": {
            "input": "$root.priceHistory",
            "as": "price",
            "in": {
                "productPrice": "$$price.productPrice",
                "priceUpdateTime": "$$price.priceUpdateTime",
            },
        },
    }


products, time_taken = get_product_data(
    "633841b402cdaa0d51efb353",
    "",
    "",
    "",
    "",
    "related",
    0,
    0,
    0,
    0,
    0,
)
print("Time taken:", time_taken)
