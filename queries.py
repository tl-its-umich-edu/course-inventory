courses_query = '''
    query coursesQuery($termID: ID!, $coursePageSize: Int, $coursePageCursor: String!) {
        term(id: $termID) {
            coursesConnection(first: $coursePageSize, after: $coursePageCursor) {
                nodes {
                    _id
                    name
                    state
                    createdAt
                    account {
                        _id
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

course_enrollments_query = '''
    query courseEnrollmentsQuery (
        $courseID: ID!,
        $enrollmentPageSize: Int,
        $enrollmentPageCursor: String
    ) {
        course(id: $courseID) {
            _id
            enrollmentsConnection(
                first: $enrollmentPageSize,
                after: $enrollmentPageCursor
            ) {
                nodes {
                    _id
                    section {
                        _id
                        name
                        updatedAt
                        createdAt
                    }
                    state
                    type
                    user {
                        _id
                        name
                        email
                        createdAt
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

queries = {
    'courses': courses_query,
    'course_enrollments': course_enrollments_query
}
