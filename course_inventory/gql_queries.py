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
                    }
                    state
                    type
                    user {
                        _id
                    }
                    course {
                        _id
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
    'course_enrollments': course_enrollments_query
}
