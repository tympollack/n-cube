package com.cedarsoftware.ncube

import java.util.regex.Pattern;

/**
 * Regular Expressions used throughout n-cube implementation.
 *
 * @author John DeRegnaucourt (jdereg@gmail.com)
 *         <br>
 *         Copyright (c) Cedar Software LLC
 *         <br><br>
 *         Licensed under the Apache License, Version 2.0 (the "License")
 *         you may not use this file except in compliance with the License.
 *         You may obtain a copy of the License at
 *         <br><br>
 *         http://www.apache.org/licenses/LICENSE-2.0
 *         <br><br>
 *         Unless required by applicable law or agreed to in writing, software
 *         distributed under the License is distributed on an "AS IS" BASIS,
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */
interface Regexes
{
    Pattern importPattern = ~/(?m)^(\s*import\s+[^;\n"'\/ ]+;?)/
    Pattern inputVar = ~/(?i)([^a-zA-Z0-9_.]|^)input[?]?[.]([a-zA-Z0-9_]+)/

    Pattern scripletPattern = ~/<%(.*?)%>/
    Pattern velocityPattern = ~/[$][{](.*?)[}]/

    Pattern validTenantName = ~/^[0-9A-Za-z-]+$/
    Pattern validBranch = ~/^[0-9A-Za-z-_.]+$/
    Pattern validVersion = ~/^\d+\.\d+\.\d+$/
    Pattern validCubeName = Pattern.compile('^[' + NCube.validCubeNameChars + ']+$')

    Pattern valid2Doubles = ~/^\s*(\-?\d+(?:\.\d+)?)\s*,\s*(\-?\d+(?:\.\d+)?)\s*$/
    Pattern valid3Doubles = ~/^\s*(\-?\d+(?:\.\d+)?)\s*,\s*(\-?\d+(?:\.\d+)?)\s*,\s*(\-?\d+(?:\.\d+)?)\s*$/

    String invalidNames = "(?!\\b(?:Author|Autowired|Basic|Column|Configuration|Controller|ControllerClass|" +
            "ControllerMethod|DiscriminatorValue|Documented|Entity|Enumerated|IdClass|InitBinder|Interface|" +
            "JoinColumns|JoinColumn|Overrride|ModelAttribute|PackageScope|PreAuthorize|RequestMapping|" +
            "RequestParam|Resource|Retention|SessionAttributes|SmartCacheCmd|SuppressFBWarnings|SuppressWarnings|" +
            "Table|Target|Temporal|XmlAnyElement|XStreamAlias)\\b.*)"
    String bracketMatch = '\\s*\\[.*?:.*?\\]'
    String varMatch = '[^)=]+'
    Pattern groovyAbsRefCubeCellPattern =  Pattern.compile('([^a-zA-Z0-9_]|^)[$]\\s*([' + NCube.validCubeNameChars + ']+)\\s*[(](' + bracketMatch + '|' + varMatch + ')[)]')
    Pattern groovyAbsRefCubeCellPatternA = Pattern.compile('([^a-zA-Z0-9_]|^)[$]\\s*([' + NCube.validCubeNameChars + ']+)\\s*(' + bracketMatch + ')')
    Pattern groovyAbsRefCellPattern =  Pattern.compile('([^a-zA-Z0-9_]|^)[$]\\s*[(](' + bracketMatch + '|' + varMatch + ')[)]')
    Pattern groovyAbsRefCellPatternA = Pattern.compile('([^a-zA-Z0-9_]|^)[$]\\s*(' + bracketMatch + ')')
    Pattern groovyRelRefCubeCellPattern =  Pattern.compile('([^a-zA-Z0-9_$]|^)@\\s*' + invalidNames + '([' + NCube.validCubeNameChars + ']+)\\s*[(](' + bracketMatch + '|' + varMatch + ')[)]')
    Pattern groovyRelRefCubeCellPatternA = Pattern.compile('([^a-zA-Z0-9_$]|^)@\\s*([' + NCube.validCubeNameChars + ']+)[\\s]*(' + bracketMatch + ')')
    Pattern groovyRelRefCellPattern =  Pattern.compile('([^a-zA-Z0-9_$]|^)@\\s*[(](' + bracketMatch + '|' + varMatch + ')[)]')
    Pattern groovyRelRefCellPatternA = Pattern.compile('([^a-zA-Z0-9_$]|^)@\\s*(' + bracketMatch + ')')
    Pattern groovyExplicitCubeRefPattern = Pattern.compile('([^a-zA-Z0-9_$]|^)NCubeManager[.]getCube\\s*[(]\\s*[\'"]([' + NCube.validCubeNameChars + ']+)[\'"]\\s*[)]')
    Pattern groovyExplicitRunRulePattern = Pattern.compile('([^a-zA-Z0-9_$]|^)runRuleCube\\s*[(]\\s*[\'"]([' + NCube.validCubeNameChars + ']+)[\'"].*?[)]')
    Pattern groovyExplicitJumpPattern = Pattern.compile('([^a-zA-Z0-9_$]|^)jump\\s*[(]\\s*[\'"]([' + NCube.validCubeNameChars + ']+)[\'"].*?[)]')

    Pattern cdnUrlPattern = ~/^\/dyn\/([^\/]+)\/(.*)$/

    Pattern hasClassDefPattern = Pattern.compile('^(|.*?\\s+)class\\s+([a-zA-Z_0-9$\\.]+).*?\\{.*?\\}.*$', Pattern.DOTALL)

    Pattern isOraclePattern = ~/(?i)^.*Oracle.*$/

    Pattern rangePattern = ~/\s*([^,]+)[,](.*)\s*$/
}
