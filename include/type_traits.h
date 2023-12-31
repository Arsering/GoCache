
namespace gbp {
/*!
 * \def DISABLE_COPY(class_name)
 *
 * \brief   A macro that disable copy of class by declaring its copy
 *          constructor and assignment operator as deleted.
 *
 * \param   class_name  Name of the class.
 */
#define DISABLE_COPY(class_name)          \
  class_name(const class_name&) = delete; \
  class_name& operator=(const class_name&) = delete;

/*!
 * \def DISABLE_MOVE(class_name)
 *
 * \brief   A macro that disable move of class by declaring its move
 *          constructor and move assignment operator as deleted.
 *
 * \param   class_name  Name of the class.
 */
#define DISABLE_MOVE(class_name)     \
  class_name(class_name&&) = delete; \
  class_name& operator=(class_name&&) = delete;

#define _F_LIKELY(condition) __builtin_expect(static_cast<bool>(condition), 1)
#define _F_UNLIKELY(condition) __builtin_expect(static_cast<bool>(condition), 0)

// struct FifoTrait {
//   using EvictionST =
// }

}  // namespace gbp