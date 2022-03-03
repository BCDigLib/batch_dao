<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    version="1.0" xmlns:ead="urn:isbn:1-931666-22-9">
    <xsl:output method="text"/>
    <xsl:variable name="varTab">
        <xsl:text>&#x9;</xsl:text>
    </xsl:variable>
    <xsl:variable name="varReturn">
        <xsl:text>&#xD;&#xA;</xsl:text>
    </xsl:variable>
    <xsl:template match="/">
        <!-- check if this EAD has file/item children -->
        <xsl:if test="//ead:dsc/ead:c">
            <xsl:for-each select="//ead:did/ead:unitid">
                <xsl:if test="ancestor::ead:c[@level='file'] or ancestor::ead:c[@level='item']" >
                    <xsl:choose>
                        <xsl:when test="not(@audience='internal')">
                            <xsl:value-of select="."/>
                            <xsl:value-of select="$varTab"/>
                            <xsl:value-of select="normalize-space(preceding-sibling::ead:unittitle)"/>
                            <xsl:value-of select="$varTab"/>
                            <xsl:text>Box </xsl:text><xsl:value-of select="following-sibling::ead:container[@type='box']"/>
                            <xsl:choose>
                                <xsl:when test="following-sibling::ead:container[@type='folder']">
                                    <xsl:text>, Folder </xsl:text><xsl:value-of select="following-sibling::ead:container[@type='folder']"/>
                                </xsl:when>
                                <xsl:when test="following-sibling::ead:container[@type='volume']">
                                    <xsl:text>, Volume </xsl:text><xsl:value-of select="following-sibling::ead:container[@type='volume']"/>
                                </xsl:when>
                            </xsl:choose>
                            <xsl:value-of select="$varTab"/>
                            <xsl:value-of select="following-sibling::ead:unitdate/@normal"/>
                            <xsl:value-of select="$varReturn"/>
                        </xsl:when>
                    </xsl:choose>
                </xsl:if>
            </xsl:for-each>
        </xsl:if>
        <!-- check if this EAD is childless -->
        <xsl:if test="not(//ead:dsc/ead:c)">
            <xsl:for-each select="//ead:archdesc[@level='collection']/ead:did/ead:unitid">
                <xsl:value-of select="."/>
                <xsl:value-of select="$varTab"/>
                <xsl:value-of select="normalize-space(preceding-sibling::ead:unittitle)"/>
                <xsl:value-of select="$varTab"/>
                <xsl:choose>
                    <xsl:when test="following-sibling::ead:container[@type='object']">
                        <xsl:text>Object </xsl:text><xsl:value-of select="following-sibling::ead:container[@type='object']"/>
                    </xsl:when>
                    <xsl:when test="following-sibling::ead:container[@type='box']">
                        <xsl:text>Box </xsl:text><xsl:value-of select="following-sibling::ead:container[@type='box']"/>
                    </xsl:when>
                    <xsl:choose>
                        <xsl:when test="following-sibling::ead:container[@type='folder']">
                            <xsl:text>, Folder </xsl:text><xsl:value-of select="following-sibling::ead:container[@type='folder']"/>
                        </xsl:when>
                        <xsl:when test="following-sibling::ead:container[@type='volume']">
                            <xsl:text>, Volume </xsl:text><xsl:value-of select="following-sibling::ead:container[@type='volume']"/>
                        </xsl:when>
                    </xsl:choose>
                </xsl:choose>
                <xsl:value-of select="$varTab"/>
                <xsl:value-of select="following-sibling::ead:unitdate/@normal"/>
                <xsl:value-of select="$varReturn"/>
            </xsl:for-each>
        </xsl:if>
    </xsl:template>
</xsl:stylesheet>