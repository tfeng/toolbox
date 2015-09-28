package me.tfeng.toolbox.spring;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * @author Thomas Feng (huining.feng@gmail.com)
 */
public class BeanUtils extends org.springframework.beans.BeanUtils {

  public static <T> Constructor<T> findConstructor(Class<T> beanClass, boolean allowNonPublic,
      Class<?>... argumentTypes) {
    Constructor<T>[] candidates =
        (Constructor<T>[]) (allowNonPublic ? beanClass.getDeclaredConstructors() : beanClass.getConstructors());
    sortConstructors(candidates);
    for (Constructor<T> candidate : candidates) {
      if (matchParameterTypes(candidate.getParameterTypes(), argumentTypes)) {
        return candidate;
      }
    }
    return null;
  }

  public static Method findMethod(Class<?> beanClass, boolean allowNonPublic, String name, Class<?>[] argumentTypes) {
    Method[] candidates = allowNonPublic ? beanClass.getDeclaredMethods() : beanClass.getMethods();
    sortMethods(candidates);
    for (Method candidate : candidates) {
      if (candidate.getName().equals(name) && matchParameterTypes(candidate.getParameterTypes(), argumentTypes)) {
        return candidate;
      }
    }
    return null;
  }

  private static int compareParameterTypes(Class<?>[] parameterTypes1, Class<?>[] parameterTypes2) {
    if (parameterTypes1.length != parameterTypes2.length) {
      return 0;
    } else {
      int result = 0;
      for (int i = 0; result == 0 && i < parameterTypes1.length; i++) {
        Class<?> parameterType1 = parameterTypes1[i];
        Class<?> parameterType2 = parameterTypes2[i];
        if (!parameterType1.equals(parameterType2)) {
          if (parameterType1.isAssignableFrom(parameterType2)) {
            result = 1;
          } else if (parameterType2.isAssignableFrom(parameterType1)) {
            result = -1;
          }
        }
      }
      return result;
    }
  }

  private static boolean matchParameterTypes(Class<?>[] parameterTypes, Class<?>[] argumentTypes) {
    if (parameterTypes.length == argumentTypes.length) {
      boolean match = true;
      for (int i = 0; i < parameterTypes.length; i++) {
        if (!parameterTypes[i].isAssignableFrom(argumentTypes[i])) {
          match = false;
          break;
        }
      }
      return match;
    } else {
      return false;
    }
  }

  private static <T> void sortConstructors(Constructor<T>[] constructors) {
    Arrays.sort(constructors, (constructor1, constructor2) ->
        compareParameterTypes(constructor1.getParameterTypes(), constructor2.getParameterTypes()));
  }

  private static <T> void sortMethods(Method[] methods) {
    Arrays.sort(methods, (method1, method2) ->
        compareParameterTypes(method1.getParameterTypes(), method2.getParameterTypes()));
  }
}
