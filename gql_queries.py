

queries = {
    'coursesQuery': '''
        query coursesQuery(
            $termID: ID!,
            $pageSize: Int,
            $pageCursor: String!
        ) 
        {
            term(id: $termID) {
                coursesConnection(
                    first: $pageSize,
                    after: $pageCursor
                ) {
                    nodes {
                        _id
                        name
                        state
                        createdAt
                        account {
                            _id
                            id
                            name
                        }
                        term {
                            _id
                            name
                        }
                    }
                    pageInfo {
                        endCursor
                        hasNextPage
                    }
                }
            }
        }
    '''
}
