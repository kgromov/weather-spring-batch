package com.kgromov.command;

import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStyle;
import org.springframework.shell.jline.PromptProvider;
import org.springframework.stereotype.Component;

@Component
public class TemperatureCommandPromptProvider implements PromptProvider{
    @Override
    public AttributedString getPrompt() {
        return new AttributedString("temperature:>", AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN));
    }
}
